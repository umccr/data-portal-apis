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

from data_portal.models import Workflow
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowHelper, EngineParametersSecondaryAnalysisHelper
from data_processors.pipeline.lambdas import wes_handler
from datetime import datetime
from data_processors.pipeline.tools.liborca import get_subject_id_from_libary_id

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

    # Set workflow helper
    wfl_helper = WorkflowHelper(WorkflowType.TUMOR_NORMAL)

    # Read input template from parameter store and populate values
    input_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_input())
    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input["output_file_prefix"] = f"{output_file_prefix}"
    workflow_input["output_directory"] = f"{output_directory}"
    workflow_input["fastq_list_rows"] = fastq_list_rows
    workflow_input["tumor_fastq_list_rows"] = tumor_fastq_list_rows

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
    workflow_version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())

    # check if a workflow is already running
    # TODO: missing criteria to uniquely match workflows to metadata
    # matched_runs = services.search_matching_runs(
    #     type_name=WorkflowType.TUMOR_NORMAL.name,
    #     wfl_id=workflow_id,
    #     version=workflow_version
    # )
    # TODO: the above will block if even one T/N workflow is running. We want to check against the SAME workflow
    matched_runs = list()

    if len(matched_runs) > 0:
        results = []
        for workflow in matched_runs:
            input_json = libjson.loads(workflow.input)

            if not input_json['output_directory'].contains(subject_id):
                # TODO: assuming the subject ID is part of the output directory. Need to find better way!
                # skip workflows of other subjects
                continue

            result = {
                'id': workflow.id,
                'wfr_id': workflow.wfr_id,
                'wfr_name': workflow.wfr_name,
                'status': workflow.end_status,
                'start': libdt.serializable_datetime(workflow.start),
            }
            results.append(result)

        if len(results):
            results_dict = {
                'status': "SKIPPED",
                'reason': "Matching workflow runs found",
                'event': libjson.dumps(event),
                'matched_runs': results
            }
            logger.info(libjson.dumps(results_dict))
            return results_dict

    # Get timestamp
    timestamp = datetime.utcnow()

    # Create engine params helper
    engine_params_obj = EngineParametersSecondaryAnalysisHelper(WorkflowType.DRAGEN_TSO_CTDNA)

    # If no running workflows were found, we proceed to preparing and kicking it off
    workflow_run_name = wfl_helper.construct_workflow_name(subject_id=subject_id)

    wfl_run = wes_handler.launch({
        'workflow_id': workflow_id,
        'workflow_version': workflow_version,
        'workflow_run_name': workflow_run_name,
        'workflow_input': workflow_input,
        'workflow_engine_parameters': engine_params_obj.get_engine_params_dict(subject_id, timestamp)
    }, context)

    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'wfr_name': workflow_run_name,
            'wfl_id': workflow_id,
            'wfr_id': wfl_run['id'],
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.TUMOR_NORMAL,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
            'sample_name': sample_name
        }
    )

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'subject_id': subject_id,
        'sample_name': workflow.sample_name,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start)
    }

    logger.info(libjson.dumps(result))

    return result
