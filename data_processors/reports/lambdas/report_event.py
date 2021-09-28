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

from data_processors.reports.services import s3_report_srv, gds_report_srv
from utils import libjson, libs3, gds

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
    """event payload dict could be S3 Object or GDS File as follows

    S3 Object
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

    GDS File
    {
        "event_type": value of GDSFilesEventType,
        "event_time": "2021-04-16T02:54:45.984000+00:00",
        "gds_volume_name": "development",
        "gds_object_meta": {
            "volumeName": "development",
            "path": "/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345.TargetRegionCoverage.json.gz",
            "eTag": "6375457e74ab46ac8f1f1f30b50fae44-1462"
            "...": "..."
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

    event_type = event['event_type']

    if "s3_bucket_name" in event:
        bucket = event['s3_bucket_name']
        key = event['s3_object_meta']['key']
        report_uri = libs3.get_s3_uri(bucket, key)
        report = s3_report_srv.persist_report(bucket, key, event_type)

    elif "gds_volume_name" in event:
        gds_volume_name = event['gds_volume_name']
        gds_path = event['gds_object_meta']['path']
        report_uri = gds.get_gds_uri(gds_volume_name, gds_path)
        report = gds_report_srv.persist_report(gds_volume_name, gds_path, event_type)

    else:
        raise ValueError("Unknown report event")

    logger.info("Report event processing complete")

    if report is not None:
        return {
            str(report.id): str(report)
        }
    else:
        return {
            f"WARN__{str(uuid.uuid4())}": f"Unable to ingest report of {event_type} for {report_uri}"
        }
