try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

# Standards
import copy
import logging
from datetime import datetime, timezone

# Data portal imports
from data_portal.models import Workflow
from data_processors.pipeline import services
from data_processors.pipeline.constant import WorkflowType, WorkflowHelper
from data_processors.pipeline.lambdas import wes_handler

# Utils imports
from utils import libjson, libssm, libdt

# Set loggers
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def sqs_handler(event, context):
    """event payload dict
    {
        'Records': [
            {
                'messageId': "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                'body': "{\"JSON\": \"Formatted Message\"}",
                'messageAttributes': {},
                'md5OfBody': "e4e68fb7bd0e697a0ae8f1bb342846b3",
                'eventSource': "aws:sqs",
                'eventSourceARN': "arn:aws:sqs:us-east-2:123456789012:fifo.fifo",
            },
            ...
        ]
    }

    Details event payload dict refer to https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    Backing queue is FIFO queue and, guaranteed delivery-once, no duplication.

    :param event:
    :param context:
    :return:
    """
    messages = event['Records']

    results = []
    for message in messages:
        job = libjson.loads(message['body'])
        results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        "sample_name": sample_name,
        "fastq_list_rows": [{
            "rgid": "index1.index2.lane",
            "rgsm": "sample_name",
            "rglb": "UnknownLibrary",
            "lane": int,
            "read_1": {
              "class": "File",
              "location": "gds://path/to/read_1.fastq.gz"
            },
            "read_2": {
              "class": "File",
              "location": "gds://path/to/read_2.fastq.gz"
            }
        }],
        "seq_run_id": "sequence run id",
        "seq_name": "sequence run name",
        "batch_run_id": "batch run id",
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.GERMLINE.name} event")
    logger.info(libjson.dumps(event))

    # Extract name of sample and the fastq list rows
    sample_name = event['sample_name']
    fastq_list_rows = event['fastq_list_rows']

    # Set sequence run id
    seq_run_id = event.get('seq_run_id', None)
    seq_name = event.get('seq_name', None)
    # Set batch run id
    batch_run_id = event.get('batch_run_id', None)

    # Set workflow helper
    wfl_helper = WorkflowHelper(WorkflowType.GERMLINE)

    # Read input template from parameter store
    input_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_input())
    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input["sample_name"] = f"{sample_name}"
    workflow_input["fastq_list_rows"] = fastq_list_rows

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
    workflow_version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())

    sqr = services.get_sequence_run_by_run_id(seq_run_id) if seq_run_id else None
    batch_run = services.get_batch_run(batch_run_id=batch_run_id) if batch_run_id else None

    matched_runs = services.search_matching_runs(
        type_name=WorkflowType.GERMLINE.name,
        wfl_id=workflow_id,
        version=workflow_version,
        sample_name=sample_name,
        sequence_run=sqr,
        batch_run=batch_run,
    )

    if len(matched_runs) > 0:
        results = []
        for workflow in matched_runs:
            result = {
                'sample_name': workflow.sample_name,
                'id': workflow.id,
                'wfr_id': workflow.wfr_id,
                'wfr_name': workflow.wfr_name,
                'status': workflow.end_status,
                'start': libdt.serializable_datetime(workflow.start),
                'sequence_run_id': workflow.sequence_run.id if sqr else None,
                'batch_run_id': workflow.batch_run.id if batch_run else None,
            }
            results.append(result)
        results_dict = {
            'status': "SKIPPED",
            'reason': "Matching workflow runs found",
            'event': libjson.dumps(event),
            'matched_runs': results
        }
        logger.info(libjson.dumps(results_dict))
        return results_dict

    # construct and format workflow run name convention
    # [RUN_NAME_PREFIX]__[WORKFLOW_TYPE]__[SEQUENCE_RUN_NAME]__[SEQUENCE_RUN_ID]__[UTC_TIMESTAMP]
    run_name_prefix = "umccr__automated"
    utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
    workflow_run_name = f"{run_name_prefix}__{WorkflowType.GERMLINE.value}__{seq_name}__{seq_run_id}__{utc_now_ts}"

    wfl_run = wes_handler.launch({
        'workflow_id': workflow_id,
        'workflow_version': workflow_version,
        'workflow_run_name': workflow_run_name,
        'workflow_input': workflow_input,
    }, context)

    workflow: Workflow = services.create_or_update_workflow(
        {
            'wfr_name': workflow_run_name,
            'wfl_id': workflow_id,
            'wfr_id': wfl_run['id'],
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.GERMLINE,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
            'sequence_run': sqr,
            'sample_name': sample_name,
            'batch_run': batch_run,
        }
    )

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'sample_name': workflow.sample_name,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
        'batch_run_id': workflow.batch_run.id if batch_run else None,
    }

    logger.info(libjson.dumps(result))

    return result
