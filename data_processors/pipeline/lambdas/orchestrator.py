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

from data_portal.models import Workflow, SequenceRun
from data_processors.pipeline import services
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import workflow_update, dispatcher
from utils import libjson

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

    # FIXME Only update workflow fow now, skip next_step auto launching while still improving implementation
    update_step(wfr_id, wfv_id, wfr_event, context)

    # FIXME temporary skip GERMLINE
    # this_workflow = update_step(wfr_id, wfv_id, wfr_event, context)  # step1 update the workflow status
    # return next_step(this_workflow, context)                         # step2 determine next step


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

    this_sqr: SequenceRun = this_workflow.sequence_run

    # depends on this_workflow state from db, we may kick off next workflow
    if this_workflow.type_name.lower() == WorkflowType.BCL_CONVERT.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():

        assert this_workflow.output is not None, f"Workflow '{this_workflow.wfr_id}' output is None"

        results = []
        for output_gds_path in parse_bcl_convert_output(this_workflow.output):
            dispatcher_result = dispatcher.handler({
                'workflow_type': WorkflowType.GERMLINE.value,
                'gds_path': output_gds_path,
                'seq_run_id': this_sqr.run_id if this_sqr else None,
                'seq_name': this_sqr.name if this_sqr else None,
            }, context)

            result = {
                'fastq_location': output_gds_path,
                'dispatcher_result': dispatcher_result
            }
            results.append(result)

        results_dict = {
            'results': results
        }

        logger.info(libjson.dumps(results_dict))

        return results_dict


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
