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
import json
from libumccr import libjson, libdt, aws
from libumccr.aws import libssm
from libumccr.aws.liblambda import LambdaInvocationType

from data_portal.models import Workflow
from data_processors.pipeline.domain.config import STAR_ALIGNMENT_LAMBDA_ARN
from data_processors.pipeline.domain.workflow import ExternalWorkflowHelper, WorkflowType
from data_processors.pipeline.services import workflow_srv, libraryrun_srv

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
    """
    star alignment event payload dict
    {
        "subject_id": subject_id,
        "sample_id": fastq_list_row.rgsm,
        "library_id": library_id,
        "fastq_fwd": fastq_list_row.read_1,
        "fastq_rev": fastq_list_row.read_2,
    }
    """
    logger.info(f"Start processing {WorkflowType.STAR_ALIGNMENT.value} event")
    logger.info(libjson.dumps(event))

    # check expected information is present
    library_id = event['library_id']
    sample_id = event['sample_id']
    subject_id = event['subject_id']
    fastq_fwd = event['fastq_fwd']
    fastq_rev = event['fastq_rev']
    assert library_id is not None
    assert sample_id is not None
    assert fastq_fwd is not None
    assert fastq_rev is not None

    # see star alignment payload for preparing job JSON structure
    # https://github.com/umccr/nextflow-stack/pull/29
    helper = ExternalWorkflowHelper(WorkflowType.STAR_ALIGNMENT)
    portal_run_id = helper.get_portal_run_id()
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
            'type': WorkflowType.STAR_ALIGNMENT,
            'input': job,
            'end_status': "CREATED",
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow([job['library_id']], workflow)

    # submit job: call star alignment lambda
    # NOTE: lambda_client and SSM parameter "should" be loaded statically on class initialisation instead of here
    # (i.e. once instead of every invocation). However, that will prevent mockito from intercepting and complicate
    # testing. We compromise the little execution overhead for ease of testing.
    lambda_client = aws.lambda_client()
    submission_lambda = libssm.get_ssm_param(STAR_ALIGNMENT_LAMBDA_ARN)
    logger.info(f"Using star alignment lambda: {submission_lambda}")
    lambda_response = lambda_client.invoke(
        FunctionName=submission_lambda,
        InvocationType=LambdaInvocationType.EVENT.value,
        Payload=json.dumps(job),
    )
    logger.info(f"Submission lambda response: {lambda_response}")

    result = {
        'portal_run_id': workflow.portal_run_id,
        'subject_id': subject_id,
        'library_id': library_id,
        'id': workflow.id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
