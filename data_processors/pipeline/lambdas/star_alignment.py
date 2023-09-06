import json

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
from libumccr import libjson, libdt, aws
from libumccr.aws import libssm
from libumccr.aws.liblambda import LambdaInvocationType

from data_portal.models import Workflow
from data_processors.pipeline.domain.config import STAR_ALIGNMENT_LAMBDA_ARN
from data_processors.pipeline.domain.workflow import ExternalWorkflowHelper, WorkflowType
from data_processors.pipeline.services import workflow_srv, libraryrun_srv

# TODO: need to find sensible data for those. Could be blank to start with and updated once we receive workflow events?
WFL_ID = "N/A"
WFV_ID = "N/A"
WF_VERSION = "N/A"
WF_STATUS = "CREATED"

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
    for message in messages:
        job = libjson.loads(message['body'])
        results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        "subject_id": subject_id,
        "sample_id": fastq_list_row.rgsm,
        "library_id": library_id,
        "fastq_fwd": fastq_list_row.read_1,
        "fastq_rev": fastq_list_row.read_2,
    }
    """
    lambda_client = aws.lambda_client()
    submission_lambda = libssm.get_ssm_param(STAR_ALIGNMENT_LAMBDA_ARN)

    logger.info(f"Start processing {WorkflowType.DRAGEN_WTS.value} event")
    logger.info(libjson.dumps(event))

    helper = ExternalWorkflowHelper(WorkflowType.STAR_ALIGNMENT)

    # prepare job input
    portal_run_id = helper.get_portal_run_id()
    # check essential information is present
    library_id = event['library_id']
    sample_id = event['sample_id']
    subject_id = event['subject_id']
    fastq_fwd = event['fastq_fwd']
    fastq_rev = event['fastq_rev']
    assert library_id is not None
    assert sample_id is not None
    assert fastq_fwd is not None
    assert fastq_rev is not None
    job = {
        "portal_run_id": portal_run_id,
        "subject_id": subject_id,
        "sample_id": sample_id,
        "library_id": library_id,
        "fastq_fwd": fastq_fwd,
        "fastq_rev": fastq_rev,
    }

    # register workflow in workflow table
    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'portal_run_id': portal_run_id,
            'wfr_name': f"star_alignment_{portal_run_id}",
            'wfr_id': f"star.{portal_run_id}",
            'wfl_id': WFL_ID,
            'wfv_id': WFV_ID,
            'version': WF_VERSION,
            'type': WorkflowType.STAR_ALIGNMENT,
            'input': job,
            # 'start': datetime.utcnow().strftime('%Y%m%d'),
            'end_status': WF_STATUS,
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow([job['library_id']], workflow)

    # submit job: call star alignment lambda
    logger.info(f"Lamdba: {submission_lambda}")
    lmbda_response = lambda_client.invoke(
        FunctionName=submission_lambda,
        InvocationType=LambdaInvocationType.EVENT.value,
        Payload=json.dumps(job),
    )
    logger.info(f"Submission lambda response: {lmbda_response}")

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
