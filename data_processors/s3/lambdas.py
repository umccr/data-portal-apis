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
from utils.libs3 import parse_raw_s3_event_records, sync_s3_event_records, filter_and_fanout

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
    records = parse_raw_s3_event_records(messages)
    results = sync_s3_event_records(records)
    # Distribute special cases to other queues
    filter_and_fanout(records);

    logger.info("S3 event processing complete")
    return results
