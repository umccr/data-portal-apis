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
from data_processors.pipeline.domain.config import SASH_LAMBDA_ARN
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
    sash event payload dict
    {
        "portal_run_id": "20230530abcdefgh",
        "subject_id": "SBJ00001",
        "tumor_sample_id": "PRJ230001",
        "tumor_library_id": "L2300001",
        "normal_sample_id": "PRJ230002",
        "normal_library_id": "L2300002",
        "dragen_somatic_dir": "gds://production/analysis_data/SBJ00001/wgs_tumor_normal/20230515zyxwvuts/L2300001_L2300002/",
        "dragen_germline_dir": "gds://production/analysis_data/SBJ00001/wgs_tumor_normal/20230515zyxwvuts/L2300002_dragen_germline/",
        "oncoanalyser_dir": "s3://org.umccr.data.oncoanalyser/analysis_data/SBJ00001/oncoanalyser/20230518poiuytre/wgs/L2300001__L2300002/SBJ00001_PRJ230001/"
    }
    """
    logger.info(f"Start processing {WorkflowType.STAR_ALIGNMENT.value} event")
    logger.info(libjson.dumps(event))

    # check expected information is present
    tumor_library_id = event['tumor_library_id']
    normal_library_id = event['normal_library_id']
    tumor_sample_id = event['tumor_sample_id']
    normal_sample_id = event['normal_sample_id']
    subject_id = event['subject_id']
    dragen_somatic_dir = event['dragen_somatic_dir']
    dragen_germline_dir = event['dragen_germline_dir']
    oncoanalyser_dir = event['oncoanalyser_dir']
    assert tumor_library_id is not None
    assert normal_library_id is not None
    assert tumor_sample_id is not None
    assert normal_sample_id is not None
    assert subject_id is not None
    assert dragen_somatic_dir is not None
    assert dragen_germline_dir is not None
    assert oncoanalyser_dir is not None

    # see sash payload for preparing job JSON structure
    # https://github.com/umccr/nextflow-stack/blob/e0878abd191b33ffbce4ab7ed72cbba1d2604262/application/pipeline-stacks/sash/lambda_functions/batch_job_submission/lambda_code.py#L20-L31
    helper = ExternalWorkflowHelper(WorkflowType.SASH)
    portal_run_id = helper.get_portal_run_id()
    job = {
        "portal_run_id": portal_run_id,
        "subject_id": subject_id,
        "tumor_sample_id": tumor_sample_id,
        "tumor_library_id": tumor_library_id,
        "normal_sample_id": normal_sample_id,
        "normal_library_id": normal_library_id,
        "dragen_somatic_dir": dragen_somatic_dir,
        "dragen_germline_dir": dragen_germline_dir,
        "oncoanalyser_dir": oncoanalyser_dir
    }

    # register workflow in workflow table
    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'portal_run_id': portal_run_id,
            'wfr_name': f"sash_{portal_run_id}",
            'type': WorkflowType.SASH,
            'input': job,
            'end_status': "CREATED",
        }
    )

    # establish link between Workflow and LibraryRun
    _ = libraryrun_srv.link_library_runs_with_x_seq_workflow([tumor_library_id, normal_library_id], workflow)

    # submit job: call sash submission lambda
    # NOTE: lambda_client and SSM parameter "should" be loaded statically on class initialisation instead of here
    # (i.e. once instead of every invocation). However, that will prevent mockito from intercepting and complicate
    # testing. We compromise the little execution overhead for ease of testing.
    lambda_client = aws.lambda_client()
    submission_lambda = libssm.get_ssm_param(SASH_LAMBDA_ARN)
    logger.info(f"Using sash submission lambda: {submission_lambda}")
    lambda_response = lambda_client.invoke(
        FunctionName=submission_lambda,
        InvocationType=LambdaInvocationType.EVENT.value,
        Payload=json.dumps(job),
    )
    logger.info(f"Submission lambda response: {lambda_response}")

    result = {
        'subject_id': subject_id,
        "tumor_library_id": tumor_library_id,
        "normal_library_id": normal_library_id,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
