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
from data_processors.pipeline.domain.config import ONCOANALYSER_WGTS_LAMBDA_ARN
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
    for message in messages:
        job = libjson.loads(message['body'])
        results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """
    expected payload
    {
        "subject_id": "SBJ00910",
        "tumor_wgs_sample_id": "PRJ230001",
        "tumor_wgs_library_id": "L2300001",
        "tumor_wgs_bam": "gds://path/to/wgs_tumor.bam",
        "tumor_wts_sample_id": "MDX210176",
        "tumor_wts_library_id": "L2100746",
        "tumor_wts_bam": "s3://path/to/tumor.bam",
        "normal_wgs_sample_id": "PRJ230003",
        "normal_wgs_library_id": "L2300003",
        "normal_wgs_bam": "gds://path/to/wgs_normal.bam",
        "existing_wgs_dir": "s3://path/to/oncoanalyser/wgs/dir/",
        "existing_wts_dir": "s3://path/to/oncoanalyser/wts/dir/",
    }
    """
    logger.info(f"Start processing {WorkflowType.ONCOANALYSER_WTS.value} event")
    logger.info(libjson.dumps(event))

    # see oncoanalyser (wgts) submission lambda payload for preparing job JSON structure
    # NOTE: WTS only requires a subset of parameters
    # https://github.com/umccr/nextflow-stack/blob/05ebc83b9a024a6db40f03a68a43228b1b6dc9ff/application/pipeline-stacks/oncoanalyser/lambda_functions/batch_job_submission/lambda_code.py#L20-L36
    # expected oncoanalyser (wgts) submission lambda payload
    # {
    #     "mode": "wgts_existing_both",
    #     "portal_run_id": "20230530abcdefgh",
    #     "subject_id": "SBJ00001",
    #     "tumor_wgs_sample_id": "PRJ230001",
    #     "tumor_wgs_library_id": "L2300001",
    #     "tumor_wgs_bam": "gds://path/to/wgs_tumor.bam",
    #     "tumor_wts_sample_id": "PRJ230002",
    #     "tumor_wts_library_id": "L2300002",
    #     "tumor_wts_bam": "s3://path/to/star_alignment.md.bam",
    #     "normal_wgs_sample_id": "PRJ230003",
    #     "normal_wgs_library_id": "L2300003",
    #     "normal_wgs_bam": "gds://path/to/wgs_normal.bam",
    #     "existing_wgs_dir": "s3://path/to/oncoanalyser/wgs/dir/",
    #     "existing_wts_dir": "s3://path/to/oncoanalyser/wts/dir/",
    # }

    helper = ExternalWorkflowHelper(WorkflowType.ONCOANALYSER_WGTS)
    portal_run_id = helper.get_portal_run_id()
    subject_id = event['subject_id']
    tumor_wgs_library_id = event['tumor_wgs_library_id']
    tumor_wts_library_id = event['tumor_wts_library_id']
    normal_wgs_library_id = event['normal_wgs_library_id']
    job = {
        "mode": "wgts_existing_both",  # hard coded for the WGTS use case with existing oncoanalyser wgs/wts results
        "portal_run_id": portal_run_id,
        "subject_id": subject_id,
        "tumor_wgs_sample_id": event['tumor_wgs_sample_id'],
        "tumor_wgs_library_id": tumor_wgs_library_id,
        "tumor_wgs_bam": event['tumor_wgs_bam'],
        "tumor_wts_sample_id": event['tumor_wts_sample_id'],
        "tumor_wts_library_id": tumor_wts_library_id,
        "tumor_wts_bam": event['tumor_wts_bam'],
        "normal_wgs_sample_id": event['normal_wgs_sample_id'],
        "normal_wgs_library_id": normal_wgs_library_id,
        "normal_wgs_bam": event['normal_wgs_bam'],
        "existing_wgs_dir": event['existing_wgs_dir'],
        "existing_wts_dir": event['existing_wts_dir']
    }

    # register workflow in workflow table
    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'portal_run_id': portal_run_id,
            'wfr_name': f"{WorkflowType.ONCOANALYSER_WGTS.value}__{portal_run_id}",
            'type': WorkflowType.ONCOANALYSER_WGTS,
            'input': job,
            'end_status': "CREATED",
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
        [tumor_wgs_library_id, tumor_wts_library_id, normal_wgs_library_id],
        workflow
    )

    # submit job: call oncoanalyser (wgts) submission lambda
    # NOTE: lambda_client and SSM parameter "should" be loaded statically on class initialisation instead of here
    # (i.e. once instead of every invocation). However, that will prevent mockito from intercepting and complicate
    # testing. We compromise the little execution overhead for ease of testing.
    lambda_client = aws.lambda_client()
    submission_lambda = libssm.get_ssm_param(ONCOANALYSER_WGTS_LAMBDA_ARN)
    logger.info(f"Using oncoanalyser (wgts) submission lambda: {submission_lambda}")
    lambda_response = lambda_client.invoke(
        FunctionName=submission_lambda,
        InvocationType=LambdaInvocationType.EVENT.value,
        Payload=json.dumps(job),
    )
    logger.info(f"Submission lambda response: {lambda_response}")

    result = {
        'portal_run_id': workflow.portal_run_id,
        'subject_id': subject_id,
        'id': workflow.id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
