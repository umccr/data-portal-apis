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

from data_portal.models import Workflow, SequenceRun, Batch, BatchRun
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


def next_step(this_workflow: Workflow, context):
    """determine next pipeline step based on this_workflow state from database

    :param this_workflow:
    :param context:
    :return: None
    """
    if not this_workflow:
        # skip if update_step has skipped
        return

    # depends on this_workflow state from db, we may kick off next workflow
    if this_workflow.type_name.lower() == WorkflowType.BCL_CONVERT.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():

        this_sqr: SequenceRun = this_workflow.sequence_run
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

    # Add the sample_library_names attribute to fastq_list_df
    fastq_list_df["sample_library_names"] = fastq_list_df['rgid'].apply(lambda x: x.rsplit('.', 1)[-1])

    # iterate through each sample group by rglb
    for sample_library_name, sample_df in fastq_list_df.groupby("sample_library_names"):

        # skip Undetermined samples
        if sample_library_name.startswith("Undetermined"):
            logger.warning(f"SKIP '{sample_library_name}' SAMPLE GERMLINE WORKFLOW LAUNCH.")
            continue

        # skip sample start with NTC_
        if sample_library_name.startswith("NTC_"):
            logger.warning(f"SKIP NTC SAMPLE '{sample_library_name}' GERMLINE WORKFLOW LAUNCH.")
            continue

        # Get sample metadata
        library_metadata: pd.DataFrame = metadata_df.query(f"sample=='{sample_library_name}'")

        # Check library metadata is not empty
        if library_metadata.empty:
            logger.warning(f"Could not get library metadata for sample '{sample_library_name}'. "
                           f"Skipping sample.")
            continue

        # Skip samples where metadata workflow is set to manual
        if library_metadata["workflow"].unique().item() == "manual":
            # We do not pursue manual samples
            logger.info(f"Skipping sample '{sample_library_name}'. "
                        f"Workflow column is set to manual for at least one library name")
            continue

        # iterate through libraries for this sample and collect their assay types
        assay_type = library_metadata["type"].unique().item()

        # skip germline if assay type is not WGS
        if assay_type != "WGS":
            logger.warning(f"SKIP {assay_type} SAMPLE '{sample_library_name}' GERMLINE WORKFLOW LAUNCH.")
            continue

        # convert read_1 and read_2 to cwl file location dict format
        sample_df["read_1"] = sample_df["read_1"].apply(cwl_file_path_as_string_to_dict)
        sample_df["read_2"] = sample_df["read_2"].apply(cwl_file_path_as_string_to_dict)

        job = {
            "sample_name": sample_library_name,
            "fastq_list_rows": sample_df.drop(columns=["sample_library_names"]).to_dict(orient="records"),
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
    """
    Convert "gds://path/to/file" to {"class": "File", "location": "gds://path/to/file"}
    :param file_path:
    :return:
    """

    return {"class": "File", "location": file_path}
