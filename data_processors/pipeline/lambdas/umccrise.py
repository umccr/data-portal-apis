try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from data_portal.models.workflow import Workflow
from data_processors.pipeline.services import workflow_srv, libraryrun_srv
from data_processors.pipeline.domain.workflow import WorkflowType, SecondaryAnalysisHelper
from data_processors.pipeline.lambdas import wes_handler

from libumccr import libjson, libdt

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
                'md5OfBody': "",
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
    batch_item_failures = []
    for message in messages:
        job = libjson.loads(message['body'])
        try:
            results.append(handler(job, context))
        except Exception as e:
            logger.exception(str(e), exc_info=e, stack_info=True)

            # SQS Implement partial batch responses - ReportBatchItemFailures
            # https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
            # https://repost.aws/knowledge-center/lambda-sqs-report-batch-item-failures
            batch_item_failures.append({
                "itemIdentifier": message['messageId']
            })

    return {
        'results': results,
        'batchItemFailures': batch_item_failures
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        "dragen_somatic_directory": {
            "class": "Directory",
            "location": "gds://path/to/somatic/output/dir"
        },
        "dragen_germline_directory": {
            "class": "Directory",
            "location": "gds://path/to/germline/output/dir"
        },
        "output_directory_name": "TumorRglb__NormalRglb",
        "subject_identifier": "SBJ01234",
        "sample_name": "TUMOR_SAMPLE_ID",
        "tumor_library_id": "tumor_rglb",
        "normal_library_id": "normal_rglb",
        "dragen_tumor_id": "tumor_rgsm",
        "dragen_normal_id": "normal_rgsm"
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.UMCCRISE.name} event")
    logger.info(libjson.dumps(event))

    subject_id = event['subject_identifier']
    sample_name = event['sample_name']  # TUMOR_SAMPLE_ID
    tumor_library_id = event['tumor_library_id']
    normal_library_id = event['normal_library_id']

    # Set workflow helper
    wfl_helper = SecondaryAnalysisHelper(WorkflowType.UMCCRISE)

    # Read input template from parameter store
    workflow_input: dict = wfl_helper.get_workflow_input()
    workflow_input['dragen_somatic_directory'] = event['dragen_somatic_directory']
    workflow_input['dragen_germline_directory'] = event['dragen_germline_directory']
    workflow_input['output_directory_name'] = event['output_directory_name']
    workflow_input['subject_identifier'] = subject_id
    workflow_input['dragen_normal_id'] = event['dragen_normal_id']
    workflow_input['dragen_tumor_id'] = event['dragen_tumor_id']

    # read workflow id and version from parameter store
    workflow_id = wfl_helper.get_workflow_id()
    workflow_version = wfl_helper.get_workflow_version()

    # construct and format workflow run name convention
    workflow_run_name = wfl_helper.construct_workflow_name(subject_id=subject_id, sample_name=tumor_library_id)
    workflow_engine_parameters = wfl_helper.get_engine_parameters(target_id=subject_id, secondary_target_id=None)

    wfl_run = wes_handler.launch({
        'workflow_id': workflow_id,
        'workflow_version': workflow_version,
        'workflow_run_name': workflow_run_name,
        'workflow_input': workflow_input,
        'workflow_engine_parameters': workflow_engine_parameters
    }, context)

    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'wfr_name': workflow_run_name,
            'wfl_id': workflow_id,
            'wfr_id': wfl_run['id'],
            'portal_run_id': wfl_helper.get_portal_run_id(),
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.UMCCRISE,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow([tumor_library_id, normal_library_id], workflow)

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'subject_id': subject_id,
        'sample_name': sample_name,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
