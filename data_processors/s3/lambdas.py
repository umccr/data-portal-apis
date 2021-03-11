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
from utils import libjson
from data_processors.s3 import helper

from data_processors.reports.services import serialize_to_cancer_report

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: dict, context) -> Union[bool, Dict[str, int]]:
    """
    Entry point for SQS event processing
    :param event: SQS event
    """
    logger.info("Start processing S3 event")
    logger.info(libjson.dumps(event))
    messages = event['Records']
    records = helper.parse_raw_s3_event_records(messages)
    results = helper.sync_s3_event_records(records)
    # TODO: Distribute special cases to other queues
    serialize_to_cancer_report(records)

    logger.info("S3 event processing complete")
    return results
