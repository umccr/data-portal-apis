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
from typing import Union, Dict

from utils import libjson, libssm, libsqs
from data_processors.s3 import helper, services

from aws_xray_sdk.core import xray_recorder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SQS_REPORT_EVENT_QUEUE_ARN = "/data_portal/backend/sqs_report_event_queue_arn"


def handler(event: dict, context) -> Union[bool, Dict[str, int]]:
    """
    Entry point for SQS event processing
    :param event: SQS event
    """

    logger.info("Start processing S3 event")
    logger.info(libjson.dumps(event))

    with xray_recorder.in_subsegment("S3_EVENT_RECORDS_TRACE") as subsegment:
        messages = event['Records']
        event_records_dict = helper.parse_raw_s3_event_records(messages)

        s3_event_records = event_records_dict['s3_event_records']
        report_event_records = event_records_dict['report_event_records']

        subsegment.put_metadata('total', len(s3_event_records), 's3_event_records')
        subsegment.put_metadata('records', s3_event_records, 's3_event_records')

        subsegment.put_metadata('total', len(report_event_records), 'report_event_records')
        subsegment.put_metadata('records', report_event_records, 'report_event_records')

        results = services.sync_s3_event_records(s3_event_records)

        if report_event_records:
            queue_arn = libssm.get_ssm_param(SQS_REPORT_EVENT_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=report_event_records, fifo=False)

    logger.info("S3 event processing complete")

    return results
