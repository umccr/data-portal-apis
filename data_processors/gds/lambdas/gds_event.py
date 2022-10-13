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
from typing import List

from dateutil.parser import parse

from libumccr import libjson
from libica.app import GDSFilesEventType

from data_processors.const import GDSEventRecord
from data_processors.gds import services

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """event payload dict
    GDS Event message wrapped in SQS message. See test_gds_event test cases for this payload format.

    :param event:
    :param context:
    :return:
    """
    logger.info("Start processing GDS event")
    logger.info(libjson.dumps(event))

    messages = event['Records']

    event_records_dict = parse_raw_gds_event_records(messages)

    gds_event_records = event_records_dict['gds_event_records']

    results = services.sync_gds_event_records(gds_event_records)

    logger.info("GDS event processing complete")

    return results


def parse_raw_gds_event_records(messages: List[dict]):
    gds_event_records = []

    for message in messages:
        event_time = parse(message['messageAttributes']['actiondate']['stringValue'])
        event_action = message['messageAttributes']['action']['stringValue']
        gds_object_meta = libjson.loads(message['body'])
        gds_volume_name = gds_object_meta['volumeName']

        event_type = GDSFilesEventType.from_value(event_action)

        gds_event_records.append(GDSEventRecord(event_type, event_time, gds_volume_name, gds_object_meta))

    return {
        'gds_event_records': gds_event_records,
    }
