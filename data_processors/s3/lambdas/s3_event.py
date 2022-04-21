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
from libumccr.aws import libssm, libsqs, libs3
from data_processors.s3 import services
from data_processors.const import ReportHelper, S3EventRecord

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
    report_event_records = event_records_dict['report_event_records']

    # subsegment.put_metadata('total', len(s3_event_records), 's3_event_records')
    # subsegment.put_metadata('records', s3_event_records, 's3_event_records')
    # subsegment.put_metadata('total', len(report_event_records), 'report_event_records')
    # subsegment.put_metadata('records', report_event_records, 'report_event_records')

    results = services.sync_s3_event_records(s3_event_records)

    if report_event_records:
        queue_arn = libssm.get_ssm_param(ReportHelper.SQS_REPORT_EVENT_QUEUE_ARN)
        libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=report_event_records, fifo=False)

    # xray_recorder.end_subsegment()

    logger.info("S3 event processing complete")

    return results


def parse_raw_s3_event_records(messages: List[dict]) -> Dict:
    """
    Parse raw SQS messages into S3EventRecord objects
    :param messages: the messages to be processed
    :return: list of S3EventRecord objects
    """
    s3_event_records = []
    report_event_records = []

    for message in messages:
        body: dict = libjson.loads(message['body'])
        if 'Records' not in body.keys():
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

            # filter early for records that need further processing with Report pipeline
            if ReportHelper.is_report(s3_object_meta['key']):
                report_event_records.append({
                    'event_type': event_type.value,
                    'event_time': event_time,
                    's3_bucket_name': s3_bucket_name,
                    's3_object_meta': s3_object_meta
                })

    return {
        's3_event_records': s3_event_records,
        'report_event_records': report_event_records
    }
