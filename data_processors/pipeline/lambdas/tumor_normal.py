try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import copy
import logging

from data_portal.models.workflow import Workflow
from data_processors.pipeline.services import workflow_srv, library_run_srv
from data_processors.pipeline.domain.workflow import WorkflowType, SecondaryAnalysisHelper
from data_processors.pipeline.lambdas import wes_handler
from data_processors.pipeline.tools.liborca import get_tiny_uuid

from utils import libjson, libssm, libdt

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def sqs_handler(event, context):
    """
    Unpack body from SQS wrapper.
    SQS event payload dict:
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
        "subject_id": "SUBJECT_ID",
        "sample_name": "TUMOR_SAMPLE_ID",
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
        "tumor_fastq_list_rows": [{
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
        "output_file_prefix": "SAMPLEID_LIBRARYID",
        "output_directory": "SAMPLEID_LIBRARYID"
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.TUMOR_NORMAL.name} event")
    logger.info(libjson.dumps(event))

    # Extract name of sample and the fastq list rows
    subject_id = event['subject_id']
    output_file_prefix = event['output_file_prefix']
    output_directory = event['output_directory']
    fastq_list_rows = event['fastq_list_rows']
    tumor_fastq_list_rows = event['tumor_fastq_list_rows']
    sample_name = event['sample_name']

    normal_library_id = fastq_list_rows[0]['rglb']
    tumor_library_id = tumor_fastq_list_rows[0]['rglb']

    # Set workflow helper
    wfl_helper = SecondaryAnalysisHelper(WorkflowType.TUMOR_NORMAL)

    # Read input template from parameter store and populate values
    input_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_input())
    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input["output_file_prefix"] = f"{output_file_prefix}"
    workflow_input["output_directory"] = f"{output_directory}_dragen"
    workflow_input["fastq_list_rows"] = fastq_list_rows
    workflow_input["tumor_fastq_list_rows"] = tumor_fastq_list_rows

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
    workflow_version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())

    # If no running workflows were found, we proceed to preparing and kicking it off
    portal_run_uuid = get_tiny_uuid()
    workflow_run_name = wfl_helper.construct_workflow_name(subject_id=subject_id,
                                                           sample_name=tumor_library_id,
                                                           portal_uuid=portal_run_uuid)
    workflow_engine_parameters = wfl_helper.get_engine_parameters(target_id=subject_id,
                                                                  secondary_target_id=None,
                                                                  portal_run_uid=portal_run_uuid)

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
            'portal_run_id': portal_run_uuid,
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.TUMOR_NORMAL,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
        }
    )

    # establish link between Workflow and LibraryRun
    _ = library_run_srv.link_library_runs_with_x_seq_workflow([tumor_library_id, normal_library_id], workflow)

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'subject_id': subject_id,
        'sample_name': sample_name,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start)
    }

    logger.info(libjson.dumps(result))

    return result
