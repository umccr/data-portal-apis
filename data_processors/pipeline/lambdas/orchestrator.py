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

from data_portal.models import Workflow, SequenceRun, Batch, BatchRun
from data_processors.pipeline import services, constant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus, FastQReadType
from data_processors.pipeline.lambdas import workflow_update, fastq
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
            return

        try:
            if this_batch.context_data is None:
                # parse bcl convert output and get all output locations
                # build a sample info and its related fastq locations
                fastq_locations = []
                for location in parse_bcl_convert_output(this_workflow.output):
                    fastq_container = fastq.handler({'gds_path': location}, None)
                    fastq_locations.append(fastq_container)

                # cache batch context data in db
                this_batch = services.update_batch(this_batch.id, context_data=fastq_locations)

            # prepare job list and dispatch to job queue
            job_list = prepare_germline_jobs(this_batch, this_batch_run, this_sqr)
            if job_list:
                queue_arn = libssm.get_ssm_param(constant.SQS_GERMLINE_QUEUE_ARN)
                libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)

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
    job_list = []
    fastq_locations: List[dict] = libjson.loads(this_batch.context_data)
    for location in fastq_locations:
        fastq_container: dict = location
        fastq_map = fastq_container['fastq_map']
        for sample_name, bag in fastq_map.items():
            fastq_list = bag['fastq_list']

            if len(fastq_list) > FastQReadType.PAIRED_END.value:
                # pair_end only at the mo, log and skip
                logger.warning(f"SKIP SAMPLE '{sample_name}' GERMLINE WORKFLOW LAUNCH. "
                               f"EXPECTING {FastQReadType.PAIRED_END.value} FASTQ FILES FOR "
                               f"{FastQReadType.PAIRED_END}. FOUND: {fastq_list}")
                continue

            job = {
                'fastq1': fastq_list[0],
                'fastq2': fastq_list[1],
                'sample_name': sample_name,
                'seq_run_id': this_sqr.run_id if this_sqr else None,
                'seq_name': this_sqr.name if this_sqr else None,
                'batch_run_id': int(this_batch_run.id)
            }

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
    :return locations: list of fastq output locations on gds
    """
    locations = []
    output: dict = libjson.loads(output_json)

    lookup_keys = ['main/fastqs', 'main/fastq-directories']
    reduced_keys = list(filter(lambda k: k in lookup_keys, output.keys()))
    if reduced_keys is None or len(reduced_keys) == 0:
        raise KeyError(f"Unexpected BCL Convert CWL output format. Excepting {lookup_keys}. Found {output.keys()}")

    for key in reduced_keys:
        main_fastqs = output[key]

        if isinstance(main_fastqs, list):
            for out in main_fastqs:
                locations.append(out['location'])

        elif isinstance(main_fastqs, dict):
            locations.append(main_fastqs['location'])

        else:
            msg = f"BCL Convert FASTQ output should be list or dict. Found type {type(main_fastqs)} -- {main_fastqs}"
            raise ValueError(msg)

    return locations
