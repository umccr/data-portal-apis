try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import uuid
import logging

from data_portal.models import Report
from data_processors.reports import services
from utils import libjson

from aws_xray_sdk.core import xray_recorder

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def sqs_handler(event, context):
    """event payload dict
    {
        'Records': [
            {
                'messageId': "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                'body': "{\"JSON\": \"Formatted Message -- For format, see in handler payload below\"}",
                'messageAttributes': {},
                'md5OfBody': "e4e68fb7bd0e697a0ae8f1bb342846b3",
                'eventSource': "aws:sqs",
                'eventSourceARN': "arn:aws:sqs:us-east-2:123456789012:my-queue",
            },
            ...
        ]
    }

    Details event payload dict refer to https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    Backing queue is Standard queue and, may deliver more than once. No order.

    :param event:
    :param context:
    :return:
    """
    messages = event['Records']
    results = []

    with xray_recorder.in_subsegment("REPORT_HANDLER_TRACE") as subsegment:
        subsegment.put_metadata('total', len(messages), 'sqs_handler')

        for message in messages:
            job = libjson.loads(message['body'])
            results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        "event_type": value of S3EventType,
        "event_time": "2021-04-16T02:54:45.984000+00:00",
        "s3_bucket_name": "primary-data-dev",
        "s3_object_meta": {
            "versionId": "ABC6i4_SXKDddofB7fvEq9z7xsvABCDE",
            "size": 170,
            "eTag": "9999ed1a461dad0af6fd8da246349999",
            "key": "cancer_report_tables/hrd/SBJ00001__SBJ00001_MDX000001_L0000001-hrdetect.json.gz",
            "sequencer": "006078FC7966A0E666"
        }
    }

    :param event:
    :param context:
    :return: dict
    """
    subsegment = xray_recorder.current_subsegment()

    logger.info("Start processing report event")
    logger.info(libjson.dumps(event))
    subsegment.put_metadata('event', event, 'handler')

    bucket = event['s3_bucket_name']
    key = event['s3_object_meta']['key']
    event_type = event['event_type']

    report = services.persist_report(bucket, key, event_type)

    logger.info("Report event processing complete")

    if report is not None and isinstance(report, Report):
        return {
            str(report.id): str(report)
        }
    else:
        return {
            f"WARN__{str(uuid.uuid4())}": f"Unable to ingest report of {event_type} for s3://{bucket}/{key}"
        }
