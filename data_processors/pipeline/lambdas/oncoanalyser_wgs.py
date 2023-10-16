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
from data_processors.pipeline.domain.config import ONCOANALYSER_WGS_LAMBDA_ARN
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
    """event payload dict for oncoanalyser wgs submission lambda
    {
        "subject_id": "SBJ00910",
        "tumor_wgs_sample_id": "MDX210176",
        "tumor_wgs_library_id": "L2100746",
        "tumor_wgs_bam": "s3://umccr-research-dev/stephen/oncoanalyser_test_data/SBJ00910/wgs/bam/GRCh38_umccr/MDX210176_tumor.bam",
        "normal_wgs_sample_id": "MDX210175",
        "normal_wgs_library_id": "L2100745",
        "normal_wgs_bam": "s3://umccr-research-dev/stephen/oncoanalyser_test_data/SBJ00910/wgs/bam/GRCh38_umccr/MDX210175_normal.bam"
    }
    """
    logger.info(f"Start processing {WorkflowType.ONCOANALYSER_WGS.value} event")
    logger.info(libjson.dumps(event))

    # check expected information is present
    subject_id = event['subject_id']
    tumor_wgs_sample_id = event['tumor_wgs_sample_id']
    tumor_wgs_library_id = event['tumor_wgs_library_id']
    tumor_wgs_bam = event['tumor_wgs_bam']
    normal_wgs_sample_id = event['normal_wgs_sample_id']
    normal_wgs_library_id = event['normal_wgs_library_id']
    normal_wgs_bam = event['normal_wgs_bam']

    # see oncoanalyser (wgs) submission lambda payload for preparing job JSON structure
    # NOTE: WGS only requires only a subset of parameters
    # https://github.com/umccr/nextflow-stack/blob/05ebc83b9a024a6db40f03a68a43228b1b6dc9ff/application/pipeline-stacks/oncoanalyser/lambda_functions/batch_job_submission/lambda_code.py#L20-L36
    helper = ExternalWorkflowHelper(WorkflowType.ONCOANALYSER_WGS)
    portal_run_id = helper.get_portal_run_id()
    job = {
        'mode': "wgs",  # hard coded for the WGS use case
        'portal_run_id': portal_run_id,
        'subject_id': subject_id,
        'tumor_wgs_sample_id': tumor_wgs_sample_id,
        'tumor_wgs_library_id': tumor_wgs_library_id,
        'tumor_wgs_bam': tumor_wgs_bam,
        'normal_wgs_sample_id': normal_wgs_sample_id,
        'normal_wgs_library_id': normal_wgs_library_id,
        'normal_wgs_bam': normal_wgs_bam
    }

    # register workflow in workflow table
    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'portal_run_id': portal_run_id,
            'wfr_name': f"{WorkflowType.ONCOANALYSER_WGS.value}__{portal_run_id}",
            'type': WorkflowType.ONCOANALYSER_WGS,
            'input': job,
            'end_status': "CREATED",
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow([tumor_wgs_library_id, normal_wgs_library_id], workflow)

    # submit job: call oncoanalyser (wgs) submission lambda
    # NOTE: lambda_client and SSM parameter "should" be loaded statically on class initialisation instead of here
    # (i.e. once instead of every invocation). However, that will prevent mockito from intercepting and complicate
    # testing. We compromise the little execution overhead for ease of testing.
    lambda_client = aws.lambda_client()
    submission_lambda = libssm.get_ssm_param(ONCOANALYSER_WGS_LAMBDA_ARN)
    logger.info(f"Using oncoanalyser (wgs) submission lambda: {submission_lambda}")
    lambda_response = lambda_client.invoke(
        FunctionName=submission_lambda,
        InvocationType=LambdaInvocationType.EVENT.value,
        Payload=json.dumps(job),
    )
    logger.info(f"Submission lambda response: {lambda_response}")

    result = {
        'portal_run_id': workflow.portal_run_id,
        'subject_id': subject_id,
        'tumor_library_id': tumor_wgs_library_id,
        'normal_library_id': normal_wgs_library_id,
        'id': workflow.id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
