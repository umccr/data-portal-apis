try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import json
import logging

from data_processors import services as srv
from data_processors.exceptions import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GDS_FILES = 'gds.files'
BSSH_RUNS = 'bssh.runs'
IMPLEMENTED_ENS_TYPES = [GDS_FILES, BSSH_RUNS]


def handler(event, context):
    logger.info("Start processing IAP ENS event")
    logger.info(event)

    messages = event['Records']

    for message in messages:
        event_type = message['messageAttributes']['type']['stringValue']

        if event_type not in IMPLEMENTED_ENS_TYPES:
            raise UnsupportedIAPEventNotificationServiceType(event_type)

        event_action = message['messageAttributes']['action']['stringValue']
        message_body_json = json.loads(message['body'])

        if event_type == GDS_FILES:
            if event_action == 'deleted':
                srv.delete_gds_file(message_body_json)
            else:
                srv.create_or_update_gds_file(message_body_json)

        if event_type == BSSH_RUNS:
            payload = {}
            payload.update(message_body_json)
            payload.update(messageAttributesAction=event_action)
            payload.update(messageAttributesActionType=event_type)
            payload.update(messageAttributesActionDate=message['messageAttributes']['actiondate']['stringValue'])
            payload.update(messageAttributesProducedBy=message['messageAttributes']['producedby']['stringValue'])
            srv.create_or_update_sequence_run(payload)

    logger.info("IAP ENS event processing complete")
