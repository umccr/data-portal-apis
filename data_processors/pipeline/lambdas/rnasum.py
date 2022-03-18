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
    for message in messages:
        job = libjson.loads(message['body'])
        results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        "dragen_transcriptome_directory": {
            "class": "Directory",
            "location": "gds://path/to/WTS/output/dir"
        },
        "umccrise_directory": {
            "class": "Directory",
            "location": "gds://path/to/umccrise/output/dir"
        },
        "arriba_directory": {
            "class": "Directory",
            "location": "gds://path/to/arriba/output/dir"
        },
        "sample_name": "TUMOR_SAMPLE_ID",
        "report_directory": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
        "dataset": "reference_data",
        "subject_id": "SUBJECT_ID",
        "tumor_library_id": "WTS_TUMOR_LIBRARY_ID"
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.RNASUM.name} event")
    logger.info(libjson.dumps(event))

    # inputs for constructing workflow run name and linking Workflow and Library run
    subject_id = event['subject_id']
    sample_name = event['sample_name']  # TUMOR_SAMPLE_ID
    tumor_library_id = event['tumor_library_id']  # WTS_TUMOR_LIBRARY_ID

    # Set workflow helper
    wfl_helper = SecondaryAnalysisHelper(WorkflowType.RNASUM)

    # Read input template from parameter store
    workflow_input: dict = wfl_helper.get_workflow_input()
    workflow_input['dragen_transcriptome_directory'] = event['dragen_transcriptome_directory']
    workflow_input['umccrise_directory'] = event['umccrise_directory']
    workflow_input['arriba_directory'] = event['arriba_directory']
    workflow_input['report_directory'] = event['report_directory']
    workflow_input['sample_name'] = sample_name

    # TCGA dataset
    # See https://github.com/umccr/RNAsum/blob/master/TCGA_projects_summary.md
    #
    # If payload dataset is not defined (i.e. Null or None) then we won't patch update the template dataset input.
    # In that case, we shall just use the dataset pre-configured in the input template (i.e. statically set dataset).
    #
    # See `rnasum_wfl_input` template that defined in parameter store
    #  https://github.com/umccr/infrastructure/blob/master/terraform/stacks/umccr_data_portal/workflow/main.tf
    #
    # NOTE:
    # Dynamically lookup of TCGA dataset implementation should be at rnasum_step module side (i.e. job producer)
    # Job consumers are typically "dumb-pipe" event handler for a smart Producer -- it is a design paradigm!
    event_payload_dataset = event.get('dataset', None)
    if event_payload_dataset is not None:
        workflow_input['dataset'] = event_payload_dataset  # override the template dataset config

    # Make sure we have set the dataset at this point
    dataset = workflow_input['dataset']
    if dataset is None:
        error_msg = f"Error invoking RNAsum workflow run Lambda. Dataset is {dataset}"
        error_result = {
            'subject_id': subject_id,
            'sample_name': sample_name,
            'tumor_library_id': tumor_library_id,
            'dataset': dataset,
            'error': error_msg,
        }
        logger.warning(libjson.dumps(error_result))
        return error_result

    logger.info(f"Using TCGA dataset: {dataset}")

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
            'type': WorkflowType.RNASUM,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow([tumor_library_id], workflow)

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'subject_id': subject_id,
        'sample_name': sample_name,
        'tumor_library_id': tumor_library_id,
        'dataset': dataset,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
