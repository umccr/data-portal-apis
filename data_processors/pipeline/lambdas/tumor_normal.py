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

from data_portal.models.workflow import Workflow
from data_processors.pipeline.services import workflow_srv, libraryrun_srv
from data_processors.pipeline.domain.workflow import WorkflowType, SecondaryAnalysisHelper
from data_processors.pipeline.lambdas import wes_handler

from libumccr import libjson, libdt

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
        "subject_id": "subjectId",
        "sample_name_germline": "normalLibraryId",
        "sample_name_somatic": "tumorLibraryId",
        "output_file_prefix_germline": "normalSampleId",
        "output_file_prefix_somatic": "tumorSampleId",
        "output_directory_germline": "normalLibraryId",
        "output_directory_somatic": "tumorLibraryId_normalLibraryId",
        "fastq_list_rows": [{
            "rgid": "index1.index2.laneNo.illuminaId.sampleId_libraryId",
            "rgsm": "sampleId",
            "rglb": "libraryId",
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
            "rgid": "index1.index2.laneNo.illuminaId.sampleId_libraryId",
            "rgsm": "sampleId",
            "rglb": "libraryId",
            "lane": int,
            "read_1": {
              "class": "File",
              "location": "gds://path/to/read_1.fastq.gz"
            },
            "read_2": {
              "class": "File",
              "location": "gds://path/to/read_2.fastq.gz"
            }
        }]
    }

    NOTE:
        For fastq_list_rows, you should typically get them from Portal /fastq endpoint
        rglb is a library_id without suffixes i.e. no _topup nor _rerun

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.TUMOR_NORMAL.value} event")
    logger.info(libjson.dumps(event))

    # Extract name of sample and the fastq list rows
    subject_id = event['subject_id']
    sample_name_germline = event['sample_name_germline']
    sample_name_somatic = event['sample_name_somatic']
    output_file_prefix_germline = event['output_file_prefix_germline']
    output_file_prefix_somatic = event['output_file_prefix_somatic']
    output_directory_germline = event['output_directory_germline']
    output_directory_somatic = event['output_directory_somatic']
    fastq_list_rows = event['fastq_list_rows']
    tumor_fastq_list_rows = event['tumor_fastq_list_rows']

    normal_library_id = fastq_list_rows[0]['rglb']
    tumor_library_id = tumor_fastq_list_rows[0]['rglb']

    # Set workflow helper
    wfl_helper = SecondaryAnalysisHelper(WorkflowType.TUMOR_NORMAL)

    workflow_input: dict = wfl_helper.get_workflow_input()
    workflow_input["output_file_prefix_germline"] = f"{output_file_prefix_germline}"
    workflow_input["output_file_prefix_somatic"] = f"{output_file_prefix_somatic}"
    workflow_input["output_directory_germline"] = f"{output_directory_germline}_dragen_germline"
    workflow_input["output_directory_somatic"] = f"{output_directory_somatic}_dragen_somatic"
    workflow_input["fastq_list_rows"] = fastq_list_rows
    workflow_input["tumor_fastq_list_rows"] = tumor_fastq_list_rows

    # read workflow id and version from parameter store
    workflow_id = wfl_helper.get_workflow_id()
    workflow_version = wfl_helper.get_workflow_version()

    # If no running workflows were found, we proceed to preparing and kicking it off
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
            'type': WorkflowType.TUMOR_NORMAL,
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
        'sample_name': sample_name_somatic,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start)
    }

    logger.info(libjson.dumps(result))

    return result
