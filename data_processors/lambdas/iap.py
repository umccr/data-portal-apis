try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# ---

import json
import logging

from data_processors.services import delete_gds_file, create_or_update_gds_file
from data_processors.exceptions import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GDS_FILES = 'gds.files'
IMPLEMENTED_ENS_TYPES = [GDS_FILES]


def handler(event, context):
    logger.info("Start processing IAP ENS event")
    logger.info(event)

    messages = event['Records']

    for message in messages:
        event_type = message['messageAttributes']['type']['stringValue']

        if event_type not in IMPLEMENTED_ENS_TYPES:
            raise UnsupportedIAPEventNotificationServiceType(event_type)

        if event_type == GDS_FILES:
            event_action = message['messageAttributes']['action']['stringValue']
            message_body_json = json.loads(message['body'])
            if event_action == 'deleted':
                delete_gds_file(message_body_json)
            else:
                create_or_update_gds_file(message_body_json)

    logger.info("IAP ENS event processing complete")
