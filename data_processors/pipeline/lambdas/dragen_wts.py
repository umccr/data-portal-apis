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
    for message in messages:
        job = libjson.loads(message['body'])
        results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        "subject_id": "subject_id",
        "library_id": "library_id (usually rglb)",
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
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.DRAGEN_WTS.value} event")
    logger.info(libjson.dumps(event))

    # Extract name of sample and the fastq list rows
    subject_id = event['subject_id']
    library_id = event['library_id']
    fastq_list_rows = event['fastq_list_rows']

    # Set sample name
    sample_name = fastq_list_rows[0]['rgsm']

    # Set workflow helper
    wfl_helper = SecondaryAnalysisHelper(WorkflowType.DRAGEN_WTS)
    workflow_input: dict = wfl_helper.get_workflow_input()
    workflow_input["output_file_prefix"] = f"{sample_name}"
    workflow_input["output_directory"] = f"{library_id}_dragen"
    workflow_input["fastq_list_rows"] = fastq_list_rows

    # read workflow id and version from parameter store
    workflow_id = wfl_helper.get_workflow_id()
    workflow_version = wfl_helper.get_workflow_version()

    # construct and format workflow run name convention
    workflow_run_name = wfl_helper.construct_workflow_name(
        sample_name=library_id,
        subject_id=subject_id
    )
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
            'portal_run_id': wfl_helper.get_portal_run_id(),
            'wfr_id': wfl_run['id'],
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.DRAGEN_WTS,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_workflow(library_id, workflow)

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'subject_id': subject_id,
        'library_id': library_id,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
