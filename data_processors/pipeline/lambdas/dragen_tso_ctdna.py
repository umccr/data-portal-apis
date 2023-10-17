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
from data_processors.pipeline.services import sequencerun_srv, batch_srv, workflow_srv, metadata_srv, libraryrun_srv
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
        "tso500_sample": {
            "sample_id": "sample_id",
            "sample_name": "sample_name",
            "sample_type": "DNA",
            "pair_id": "sample_name",
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
            }]
        },
        ,
        "samplesheet": {
          "class": "File",
          "location": "gds://path/to/samplesheet"
        },
        "run_info_xml": {
          "class": "File",
          "location": "path/to/dir/RunInfo.xml"
        },
        "run_parameters_xml": {
          "class": "File",
          "location": "path/to/dir/RunParameters.xml"
        },
        "seq_run_id": "sequence run id",
        "seq_name": "sequence run name",
        "batch_run_id": "batch run id",
        "library_id": "library id",
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.DRAGEN_TSO_CTDNA.value} event")
    logger.info(libjson.dumps(event))

    seq_run_id = event.get('seq_run_id', None)
    seq_name = event.get('seq_name', None)
    batch_run_id = event.get('batch_run_id', None)
    library_id = event['library_id']

    # Set workflow helper
    wfl_helper = SecondaryAnalysisHelper(WorkflowType.DRAGEN_TSO_CTDNA)

    # Read input template from parameter store
    workflow_input: dict = wfl_helper.get_workflow_input()
    workflow_input['tso500_samples'] = [event['tso500_sample']]
    workflow_input['samplesheet'] = event['samplesheet']
    workflow_input['run_info_xml'] = event['run_info_xml']
    workflow_input['run_parameters_xml'] = event['run_parameters_xml']

    # read workflow id and version from parameter store
    workflow_id = wfl_helper.get_workflow_id()
    workflow_version = wfl_helper.get_workflow_version()

    sqr = sequencerun_srv.get_sequence_run_by_run_id(seq_run_id) if seq_run_id else None
    batch_run = batch_srv.get_batch_run(batch_run_id=batch_run_id) if batch_run_id else None

    # construct and format workflow run name convention
    subject_id = metadata_srv.get_subject_id_from_library_id(library_id)
    workflow_run_name = wfl_helper.construct_workflow_name(sample_name=library_id, subject_id=subject_id)
    workflow_engine_parameters = wfl_helper.get_engine_parameters(target_id=subject_id, secondary_target_id=library_id)

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
            'type': WorkflowType.DRAGEN_TSO_CTDNA,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
            'sequence_run': sqr,
            'batch_run': batch_run,
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_workflow(library_id, workflow)

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'library_id': library_id,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
        'batch_run_id': workflow.batch_run.id if batch_run else None,
    }

    logger.info(libjson.dumps(result))

    return result
