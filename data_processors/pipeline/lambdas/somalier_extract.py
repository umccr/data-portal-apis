try:
    import unzip_requirements
except ImportError:
    pass

import os
import json
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from libumccr import libjson, libdt
import boto3
from data_processors.pipeline.services.somalier_check_srv import \
    get_fingerprint_extraction_service_instance
from datetime import datetime
from urllib.parse import urlparse

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
        "gds_path": "gds://path/to/bam/file"
    }

    :param event:
    :param context:
    :return: dict response of the aws step function launched
    """

    logger.info(f"Start processing calling somalier extract")
    logger.info(libjson.dumps(event))

    # Extract name of sample and the fastq list rows
    gds_path = event['gds_path']

    # Call somalier check step
    somalier_extract_step_function = get_fingerprint_extraction_service_instance()

    # Get name
    timestamp = int(datetime.now().replace(microsecond=0).timestamp())
    step_function_instance_name = "__".join([
        "somalier_extract",
        urlparse(gds_path).path.lstrip("/").replace("/", "_").rstrip(".bam")[-40:],
        str(timestamp)
    ])

    # Call check step function
    client = boto3.client('stepfunctions')
    step_function_instance_obj = client.start_execution(
        stateMachineArn=somalier_extract_step_function,
        name=step_function_instance_name,
        input=json.dumps({
            "needsFingerprinting": [
                [
                    gds_path
                ]
            ]
        })
    )

    # Get execution arn
    somalier_extract_execution_arn = step_function_instance_obj.get('executionArn', None)
    logger.info(f"Extracting fingerprint from '{gds_path}' with "
                f"step function instance '{somalier_extract_execution_arn}'")

    logger.info(libjson.dumps(step_function_instance_obj))

    return step_function_instance_obj
