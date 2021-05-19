try:
    import unzip_requirements
except ImportError:
    pass

import os

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
from typing import List
import pandas as pd

from data_portal.models import Workflow, SequenceRun, Batch, BatchRun, LabMetadata, LabMetadataType, \
    LabMetadataPhenotype, FastqListRow
from data_processors.pipeline import services, constant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import workflow_update, fastq_list_row, demux_metadata
from utils import libjson, libsqs, libssm

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """event payload dict
    {
        'wfr_id': "wfr.xxx",
        'wfv_id': "wfv.xxx",
        'wfr_event': {
            'event_type': "RunSucceeded",
            'event_details': {},
            'timestamp': "2020-06-24T11:27:35.1268588Z"
        }
    }

    :param event:
    :param context:
    :return: None
    """

    logger.info(f"Start processing workflow orchestrator event")
    logger.info(libjson.dumps(event))

    wfr_id = event['wfr_id']
    wfv_id = event['wfv_id']
    wfr_event = event.get('wfr_event')  # wfr_event is optional

    this_workflow = update_step(wfr_id, wfv_id, wfr_event, context)  # step1 update the workflow status
    return next_step(this_workflow, context)                         # step2 determine next step


def update_step(wfr_id, wfv_id, wfr_event, context):
    # update workflow run output, end time, end status and notify if necessary
    updated_workflow: dict = workflow_update.handler({
        'wfr_id': wfr_id,
        'wfv_id': wfv_id,
        'wfr_event': wfr_event,
    }, context)

    if updated_workflow:
        this_workflow: Workflow = services.get_workflow_by_ids(
            wfr_id=updated_workflow['wfr_id'],
            wfv_id=updated_workflow['wfv_id']
        )
        return this_workflow

    return None


def get_library_id_from_workflow(workflow: Workflow):
    # extract library ID from Germline WF sample_name
    # TODO: is there a better way? Could use the fastq_list_row entries of the workflow 'input'

    # remove the first part (sample ID) from the sample_name to get the library ID
    # NOTE: this may not be exactly the library ID used in the Germline workflow (stripped off _topup/_rerun), but
    #       for our use case that does not matter, as we are merging all libs from the same subject anyway
    return '_'.join(workflow.sample_name.split('_')[1:])


def get_subjects_from_runs(workflows: list) -> list:
    subjects = set()
    for workflow in workflows:
        # use workflow helper class to extract sample/library ID from workflow
        library_id = get_library_id_from_workflow(workflow)
        if library_id:
            # use metadata helper class to get corresponding subject ID
            subject_id = LabMetadata.objects.get(library_id=library_id).subject_id
            if subject_id:
                subjects.add(subject_id)
            else:
                raise ValueError(f"No subject for library {library_id}")
        else:
            raise ValueError(f"Could not extract LibraryID from workflow {workflow.wfr_id}")

    return list(subjects)


def extract_sample_library_ids(fastq_list_rows: List[FastqListRow]):
    samples = set()
    libraries = set()

    for row in fastq_list_rows:
        libraries.add(row.rglb)
        samples.add(row.rgsm)

    return list(samples), list(libraries)


def prepare_tumor_normal_jobs(subjects: List[str]) -> list:
    jobs = list()
    for subject in subjects:
        job = create_tn_job(subject)
        if job:
            jobs.append(job)
        else:
            logger.debug(f"Not running T/N workflow for {subject}. Conditions not met.")

    return jobs


def create_tn_job(subject_id: str) -> dict:
    # TODO: could query for all records in one go and then filter locally
    # records = LabMetadata.objects.filter(subject_id=subject_id)

    # extract tumor/normal pairs for a subject (taking into account Type & Phenotype)
    tumor_records = LabMetadata.objects.filter(
        subject_id=subject_id,
        type__iexact=LabMetadataType.WGS.value.lower(),
        phenotype__iexact=LabMetadataPhenotype.TUMOR.value.lower())

    normal_records = LabMetadata.objects.filter(
        subject_id=subject_id,
        type__iexact=LabMetadataType.WGS.value.lower(),
        phenotype__iexact=LabMetadataPhenotype.NORMAL.value.lower())

    # TODO: sort out topup/rerun logic

    # check if we have records for both phenotypes
    # entries for tumor and normal don't have to happen at the same time, and in some cases we don't have both at all
    if len(tumor_records) == 0 or len(normal_records) == 0:
        logger.warning(f"Skipping subject {subject_id} (tumor or normal lib still missing).")
        return {}

    # now that we found the metadata records check if FASTQs are available
    tumor_fastq_list_rows = list()
    for record in tumor_records:
        fastq_rows = FastqListRow.objects.filter(rglb=record.library_id)
        tumor_fastq_list_rows.extend(fastq_rows)
    if len(tumor_fastq_list_rows) < 1:
        # TODO: legacy data might be not in fastq_list_row table. Could fall back to alternative lookup
        logger.debug(f"Skipping subject {subject_id} (tumor FASTQs still missing).")
        return {}

    normal_fastq_list_rows: List[FastqListRow] = list()
    for record in normal_records:
        fastq_rows = FastqListRow.objects.filter(rglb=record.library_id)
        normal_fastq_list_rows.extend(fastq_rows)
    if len(normal_fastq_list_rows) < 1:
        # TODO: legacy data might be not in fastq_list_row table. Could fall back to alternative lookup
        logger.debug(f"Skipping subject {subject_id} (normal FASTQs still missing).")
        return {}

    # quick check: at this point we'd expect one library/sample for each normal/tumor
    # NOTE: IDs are from rglb/rgsm of FastqListRow, so library IDs are stripped of _topup/_rerun extensions
    # TODO: handle other cases
    n_samples, n_libraries = extract_sample_library_ids(normal_fastq_list_rows)
    logger.info(f"Normal samples/Libraries for subject {subject_id}: {n_samples}/{n_libraries}")
    t_samples, t_libraries = extract_sample_library_ids(tumor_fastq_list_rows)
    logger.info(f"Tumor samples/Libraries for subject {subject_id}: {t_samples}/{t_libraries}")
    if len(n_samples) != 1 or len(n_libraries) != 1:
        raise ValueError(f"Unexpected number of normal samples!")
    if len(t_samples) != 1 or len(t_libraries) != 1:
        raise ValueError(f"Unexpected number of tumor samples!")

    tumor_sample_id = t_samples[0]

    # hacky way to convert non-serializable Django Model objects to the Json format we expect
    # TODO: find a better way to define a Json Serializer for Django Model objects
    normal_dict_list = list()
    for row in normal_fastq_list_rows:
        normal_dict_list.append(row.to_dict())
    tumor_dict_list = list()
    for row in tumor_fastq_list_rows:
        tumor_dict_list.append(row.to_dict())

    # create T/N job definition
    job_json = {
        "subject_id": subject_id,
        "fastq_list_rows": normal_dict_list,
        "tumor_fastq_list_rows": tumor_dict_list,
        "output_file_prefix": tumor_sample_id,
        "output_directory": subject_id
    }

    return job_json


def next_step(this_workflow: Workflow, context):
    """determine next pipeline step based on this_workflow state from database

    :param this_workflow:
    :param context:
    :return: None
    """
    if not this_workflow:
        # skip if update_step has skipped
        return

    this_sqr: SequenceRun = this_workflow.sequence_run
    # depends on this_workflow state from db, we may kick off next workflow
    if this_workflow.type_name.lower() == WorkflowType.BCL_CONVERT.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():

        # a bcl convert workflow run association to a sequence run is very strong and
        # those logic impl this point onward depends on its attribute like sequence run name
        if this_sqr is None:
            raise ValueError(f"Workflow {this_workflow.type_name} wfr_id: '{this_workflow.wfr_id}' must be associated "
                             f"with a SequenceRun. Found SequenceRun is: {this_sqr}")

        # bcl convert workflow run must have output in order to continue next step
        if this_workflow.output is None:
            raise ValueError(f"Workflow '{this_workflow.wfr_id}' output is None")

        # create a batch if not exist
        batch_name = this_sqr.name if this_sqr else f"{this_workflow.type_name}__{this_workflow.wfr_id}"
        this_batch = services.get_or_create_batch(name=batch_name, created_by=this_workflow.wfr_id)

        # register a new batch run for this_batch run step
        this_batch_run = services.skip_or_create_batch_run(
            batch=this_batch,
            run_step=WorkflowType.GERMLINE.value.upper()
        )
        if this_batch_run is None:
            # skip the request if there is on going existing batch_run for the same batch run step
            # this is especially to fence off duplicate IAP WES events hitting multiple time to our IAP event lambda
            msg = f"SKIP. THERE IS EXISTING ON GOING RUN FOR BATCH " \
                  f"ID: {this_batch.id}, NAME: {this_batch.name}, CREATED_BY: {this_batch.created_by}"
            logger.warning(msg)
            return {'message': msg}

        try:
            if this_batch.context_data is None:
                # parse bcl convert output and get all output locations
                # build a sample info and its related fastq locations
                fastq_list_rows: List = fastq_list_row.handler({
                    'fastq_list_rows': parse_bcl_convert_output(this_workflow.output),
                    'seq_name': this_sqr.name,
                }, None)

                # cache batch context data in db
                this_batch = services.update_batch(this_batch.id, context_data=fastq_list_rows)

                # Initialise fastq list rows object in model
                for row in fastq_list_rows:
                    services.create_or_update_fastq_list_row(row, this_sqr)

            # prepare job list and dispatch to job queue
            job_list = prepare_germline_jobs(this_batch, this_batch_run, this_sqr)
            if job_list:
                queue_arn = libssm.get_ssm_param(constant.SQS_GERMLINE_QUEUE_ARN)
                libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
            else:
                services.reset_batch_run(this_batch_run.id)  # reset running if job_list is empty

        except Exception as e:
            services.reset_batch_run(this_batch_run.id)  # reset running
            raise e

        return {
            'batch_id': this_batch.id,
            'batch_name': this_batch.name,
            'batch_created_by': this_batch.created_by,
            'batch_run_id': this_batch_run.id,
            'batch_run_step': this_batch_run.step,
            'batch_run_status': "RUNNING" if this_batch_run.running else "NOT_RUNNING"
        }
    elif this_workflow.type_name.lower() == WorkflowType.GERMLINE.value.lower():
        # check if all other Germline workflows for this run have finished
        # if yes we continue to the T/N workflow
        # if not, we wait (until all Germline workflows have finished)
        running: List[Workflow] = services.get_germline_running_by_sequence_run(sequence_run=this_sqr)
        succeeded: List[Workflow] = services.get_germline_succeeded_by_sequence_run(sequence_run=this_sqr)
        subjects = list()
        if len(running) == 0:
            # determine which samples are available for T/N wokflow
            subjects = get_subjects_from_runs(succeeded)
            job_list = prepare_tumor_normal_jobs(subjects=subjects)
            if job_list:
                queue_arn = libssm.get_ssm_param(constant.SQS_TN_QUEUE_ARN)
                libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
        else:
            logger.debug(f"Germline workflow finished, but {len(running)} still running. Wait for them to finish...")

        return {
            "subjects": subjects
        }


def prepare_germline_jobs(this_batch: Batch, this_batch_run: BatchRun, this_sqr: SequenceRun) -> List[dict]:
    """
    NOTE: as of GERMLINE CWL workflow version 3.7.5--1.3.5, it uses fastq_list_rows format
    See Example IAP Run > Inputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.5cc28c147e4e4dfa9e418523188aacec/3.7.5--1.3.5.md

    Germline job preparation is at _pure_ Library level aggregate.
    Here "Pure" Library ID means without having _topup(N) or _rerun(N) suffixes.
    The fastq_list_row lambda already stripped these topup/rerun suffixes (i.e. what is in this_batch.context_data cache).
    Therefore, it aggregates all fastq list at
        - per sequence run by per library for
            - all different lane(s)
            - all topup(s)
            - all rerun(s)
    This constitute one Germline job (i.e. one Germline workflow run).

    See OrchestratorIntegrationTests.test_prepare_germline_jobs() for example job list of SEQ-II validation run.

    :param this_batch:
    :param this_batch_run:
    :param this_sqr:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(this_batch.context_data)

    # get metadata for determining which sample need to be run through the germline workflow
    metadata: dict = demux_metadata.handler({
        'gdsVolume': this_sqr.gds_volume_name,
        'gdsBasePath': this_sqr.gds_folder_path,
        'gdsSamplesheet': this_sqr.sample_sheet_name,
    }, None)

    metadata_df = pd.DataFrame(metadata)
    fastq_list_df = pd.DataFrame(fastq_list_rows)

    # iterate through each sample group by rglb
    for rglb, sample_df in fastq_list_df.groupby("rglb"):

        rgsm = sample_df['rgsm'].unique().item()  # get rgsm which should be the same for all libraries

        sample_name = f"{rgsm}_{rglb}"  # this is now "sample name" convention for analysis workflow perspective

        # skip Undetermined samples
        if sample_name.startswith("Undetermined"):
            logger.warning(f"SKIP '{sample_name}' SAMPLE GERMLINE WORKFLOW LAUNCH.")
            continue

        # skip sample start with NTC_
        if sample_name.startswith("NTC_"):
            logger.warning(f"SKIP NTC SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH.")
            continue

        # collect back BSSH run styled SampleSheet(SampleID) globally unique ID format from rgid
        sample_library_names = list(map(lambda k: k.split('.')[-1], sample_df.rgid.unique().tolist()))

        # Skip samples where metadata workflow is set to manual
        is_manual = False
        for sample_library_name in sample_library_names:
            library_metadata: pd.DataFrame = metadata_df.query(f"sample=='{sample_library_name}'")
            if not library_metadata.empty and library_metadata["workflow"].unique().item() == "manual":
                logger.info(f"Skipping sample '{sample_name}'. Workflow column for matching "
                            f"sample '{sample_library_name}' is set to manual")
                is_manual = True
                break

        # Break out of manual
        if is_manual:
            # We do not pursue manual samples
            logger.info(f"Skipping sample '{sample_name}'. "
                        f"Workflow column is set to manual for at least one library name")
            continue

        # iterate through libraries for this sample and collect their assay types
        assay_types = []
        for sample_library_name in sample_library_names:
            library_metadata: pd.DataFrame = metadata_df.query(f"sample=='{sample_library_name}'")
            if not library_metadata.empty:
                assay_types.append(library_metadata["type"].unique().item())

        # ensure there are some assay types for this sample
        if len(set(assay_types)) == 0:
            logger.warning(f"SKIP SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH. NO ASSAY TYPE METADATA FOUND.")
            continue

        # ensure only one assay type
        if not len(set(assay_types)) == 1:
            logger.warning(f"SKIP SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH. MULTIPLE ASSAY TYPES: {assay_types}")
            continue

        # now we assign this _single_ assay type
        assay_type = list(set(assay_types))[0]

        # skip germline if assay type is not WGS
        if assay_type != "WGS":
            logger.warning(f"SKIP {assay_type} SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH.")
            continue

        # convert read_1 and read_2 to cwl file location dict format
        sample_df["read_1"] = sample_df["read_1"].apply(cwl_file_path_as_string_to_dict)
        sample_df["read_2"] = sample_df["read_2"].apply(cwl_file_path_as_string_to_dict)

        job = {
            "sample_name": sample_name,
            "fastq_list_rows": sample_df.to_dict(orient="records"),
            "seq_run_id": this_sqr.run_id if this_sqr else None,
            "seq_name": this_sqr.name if this_sqr else None,
            "batch_run_id": int(this_batch_run.id)
        }

        job_list.append(job)

    return job_list


def parse_bcl_convert_output(output_json: str) -> list:
    """
    NOTE: as of BCL Convert CWL workflow version 3.7.5, it uses fastq_list_rows format
    Given bcl convert workflow output json, return fastq_list_rows
    See Example IAP Run > Outputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.84abc203cabd4dc196a6cf9bb49d5f74/3.7.5.md

    :param output_json: workflow run output in json format
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """
    output: dict = libjson.loads(output_json)

    lookup_keys = ['main/fastq_list_rows', 'fastq_list_rows']  # lookup in order, return on first found
    look_up_key = None
    for k in lookup_keys:
        if k in output.keys():
            look_up_key = k
            break

    if look_up_key is None:
        raise KeyError(f"Unexpected BCL Convert CWL output format. Expecting one of {lookup_keys}. Found {output.keys()}")

    return output[look_up_key]


def cwl_file_path_as_string_to_dict(file_path):
    # TODO: extract to global util?
    """
    Convert "gds://path/to/file" to {"class": "File", "location": "gds://path/to/file"}
    :param file_path:
    :return:
    """

    return {"class": "File", "location": file_path}
