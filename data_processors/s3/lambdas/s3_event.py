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
from typing import Union, Dict, List
from dateutil.parser import parse

from libumccr import libjson
from libumccr.aws import libs3
from data_processors.s3 import services
from data_processors.const import S3EventRecord

# from aws_xray_sdk.core import xray_recorder

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: dict, context) -> Union[bool, Dict[str, int]]:
    """
    S3 Event message wrapped in SQS message. See test_s3_event test cases for this payload format.

    :param event:
    :param context:
    :return:
    """

    logger.info("Start processing S3 event")
    logger.info(libjson.dumps(event))

    # subsegment = xray_recorder.begin_subsegment("S3_EVENT_RECORDS_TRACE")

    messages = event['Records']

    event_records_dict = parse_raw_s3_event_records(messages)

    s3_event_records = event_records_dict['s3_event_records']

    # subsegment.put_metadata('total', len(s3_event_records), 's3_event_records')
    # subsegment.put_metadata('records', s3_event_records, 's3_event_records')

    results = services.sync_s3_event_records(s3_event_records)

    # xray_recorder.end_subsegment()

    logger.info("S3 event processing complete")

    return results


def parse_raw_s3_event_records(messages: List[dict]) -> Dict:
    """
    Parse raw SQS messages into internal S3EventRecord struct representation.

    1) Bucket notification > SQS > Lambda
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html

    2) Bucket > EventBridge enable > SQS > Lambda
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventBridge.html
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/ev-events.html

    :param messages: the messages to be processed
    :return: list of S3EventRecord objects
    """
    s3_event_records = []

    for message in messages:
        body: dict = libjson.loads(message['body'])

        if 'detail-type' in body:

            # -- S3 event route through EventBridge integration

            event_name = str(body['detail-type']).strip().replace(' ', '')
            event_time = parse(body['time'])
            s3 = body['detail']
            s3_bucket_name = s3['bucket']['name']
            s3_object_meta = s3['object']

            if event_name in ['ObjectCreated']:
                event_type = libs3.S3EventType.EVENT_OBJECT_CREATED
            elif event_name in ['ObjectDeleted']:
                event_type = libs3.S3EventType.EVENT_OBJECT_REMOVED
            else:
                event_type = libs3.S3EventType.EVENT_UNSUPPORTED

            logger.debug(f"Found new event of type {event_type}")

            s3_event_records.append(S3EventRecord(event_type, event_time, s3_bucket_name, s3_object_meta))

        else:

            # -- S3 event route through SQS, SNS, Lambda integration

            if 'Records' not in body:
                continue

            records = body['Records']

            for record in records:
                event_name = record['eventName']
                event_time = parse(record['eventTime'])
                s3 = record['s3']
                s3_bucket_name = s3['bucket']['name']
                s3_object_meta = s3['object']

                # Check event type
                if libs3.S3EventType.EVENT_OBJECT_CREATED.value in event_name:
                    event_type = libs3.S3EventType.EVENT_OBJECT_CREATED
                elif libs3.S3EventType.EVENT_OBJECT_REMOVED.value in event_name:
                    event_type = libs3.S3EventType.EVENT_OBJECT_REMOVED
                else:
                    event_type = libs3.S3EventType.EVENT_UNSUPPORTED

                logger.debug(f"Found new event of type {event_type}")

                s3_event_records.append(S3EventRecord(event_type, event_time, s3_bucket_name, s3_object_meta))

    return {
        's3_event_records': s3_event_records,
    }
