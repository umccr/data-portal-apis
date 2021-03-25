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
                fastq_list_rows = fastq_list_row.handler({'fastq_list_rows': parse_bcl_convert_output(this_workflow.output),
                                                          'sequence_run_id': this_sqr.run_id}, None)

                # cache batch context data in db
                this_batch = services.update_batch(this_batch.id, context_data=fastq_list_rows)

                # Initialise fastq list rows object in model
                for row in fastq_list_rows:
                    services.create_fastq_list_row(row,
                                                   sequence_run=this_sqr.run_id)

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
    NOTE: as of GERMLINE CWL workflow version 0.2-inputcsv-redir-19ddeb3

    GERMLINE CWL workflow only support _single_ FASTQ directory and _single_ fastq_list.csv, See:
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/production/wfl.d6f51b67de5b4d309dddf4e411362be7/0.2-inputcsv-redir-19ddeb3.md
    https://github.com/umccr-illumina/cwl-iap/blob/19ddeb38f89bbd8d6ba2b72a7da6c9fe51145fa5/workflows/dragen-qc-hla/0.2/dragen-qc-hla-inputCSV.redirect.cwl#L59

    Portal fastq lambda is now able to construct FASTQ listing from multiple gds locations, See:
    https://github.com/umccr/data-portal-apis/pull/137

    Since downstream CWL workflow cannot take multiple fastq_directories and fastq_list_csv,
    hence, skipping if Portal detect this.

    :param this_batch:
    :param this_batch_run:
    :param this_sqr:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(this_batch.context_data)

    # Convert to pandas dataframe
    fastq_list_df = pd.DataFrame(fastq_list_rows)

    # Get metadata for determining which sample types need to be run through
    # the germline workflow
    gds_volume_name = this_sqr.gds_volume_name
    gds_folder_path = this_sqr.gds_folder_path
    sample_sheet_name = this_sqr.sample_sheet_name

    metadata: dict = demux_metadata.handler({
        'gdsVolume': gds_volume_name,
        'gdsBasePath': gds_folder_path,
        'gdsSamplesheet': sample_sheet_name,
    }, None)

    # Easier to handle this as a df
    metadata_df = pd.DataFrame(metadata)

    # Iterate through each sample by grouping by the RGSM value
    for sample_name, sample_df in fastq_list_df.groupby("rgsm"):
        # skip Undetermined samples
        if sample_name == "Undetermined":
            logger.warning(f"SKIP '{sample_name}' SAMPLE GERMLINE WORKFLOW LAUNCH.")
            continue

        # skip sample start with NTC_
        if sample_name.startswith("NTC_"):
            logger.warning(f"SKIP NTC SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH.")
            continue

        # Iterate through libraries for this sample, ensure that they're the same type
        sample_library_names = ["{}_{}".format(sample_name, sample_library)
                                 for sample_library in sample_df.rglb.unique().tolist()]
        assay_types = []

        # Assign assay types by library
        for sample_library_name in sample_library_names:
            assay_types.append(metadata_df.query("sample==\"{}\"".format(sample_library_name))["type"].unique().item())

        # Ensure no assay types
        if list(set(assay_types)) == 0:
            logger.warning("Skipping sample \"{}\", could not retrieve the assay type from the metadata dict".format(
                sample_name
            ))
            continue

        # Ensure only one assay type
        if not list(set(assay_types)) == 1:
            logger.warning("Skipping sample \"{}\", received multiple assay types: \"{}\"".format(
                sample_name, ", ".join(assay_types)
            ))
            continue

        # Assign the single assay type
        assay_type = list(set(assay_types))[0]

        if assay_type != "WGS":
            logger.warning(f"SKIP {assay_type} SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH.")
            continue

        # Convert read_1 and read_2 to dicts
        sample_df["read_1"] = sample_df["read_1"].apply(cwl_file_path_as_string_to_dict)
        sample_df["read_2"] = sample_df["read_2"].apply(cwl_file_path_as_string_to_dict)

        # Initialise job
        job = {
            "sample_name": sample_name,
            "fastq_list_rows": sample_df.to_dict(orient="records"),
            "seq_run_id": this_sqr.run_id if this_sqr else None,
            "seq_name": this_sqr.name if this_sqr else None,
            "batch_run_id": int(this_batch_run.id)
        }

        # Append job to job_list
        job_list.append(job)

    return job_list


def parse_bcl_convert_output(output_json: str) -> list:
    """
    Given this_workflow (bcl convert) output (fastqs), return the list of fastq locations on gds

    BCL Convert CWL output may be Directory or Directory[], See:
    [1]: https://www.commonwl.org/v1.0/CommandLineTool.html#Directory
    [2]: https://github.com/umccr-illumina/cwl-iap/blob/5ebe927b885a6f6d18ed220dba913d08eb45a67a/workflows/bclconversion/bclConversion-main.cwl#L30
    [3]: https://github.com/umccr-illumina/cwl-iap/blob/1263e9d43cf08cfb7438dcfe42166e88b8456e54/workflows/bclconversion/1.0.4/bclConversion-main.cwl#L69
    [4]: https://github.com/umccr-illumina/cwl-iap/blob/5b96a8cfafa5ac515ca46cf67aa20b40a716b4bd/workflows/bclconversion/1.0.6/bclConversion-main.1.0.6.cwl#L167

    :param output_json: workflow run output in json format
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """
    output: dict = libjson.loads(output_json)

    look_up_key = 'main/fastq_list_rows'
    if look_up_key not in output.keys():
        raise KeyError(f"Unexpected BCL Convert CWL output format. Expecting {look_up_key}. Found {output.keys()}")

    return output[look_up_key]


def cwl_file_path_as_string_to_dict(file_path):
    """
    Convert "gds://path/to/file" to {"class": "File", "location": "gds://path/to/file"}
    :param file_path:
    :return:
    """

    return {"class": "File", "location": file_path}
