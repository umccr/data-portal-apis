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
from typing import Dict

from data_processors.lims.services import persist_lims_data_from_google_drive
from utils import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def scheduled_update_handler(event, context) -> Dict[str, int]:
    """
    Handler for LIMS update by reading the designated Spreadsheet file from Google Drive.
    Can be hit by Cron like job scheduler. It uses EventBridge on default EventBus with cron(0 12 * * ? *). See REF.
    Can be also invoked directly:
        aws lambda invoke --function-name data-portal-api-[dev|prod]-lims_scheduled_update_processor

    REF:
    https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html
    https://docs.aws.amazon.com/eventbridge/latest/userguide/event-types.html#schedule-event-type
    https://docs.aws.amazon.com/eventbridge/latest/userguide/run-lambda-schedule.html

    :param event:
    :param context:
    :return: stat of LIMS update
    """
    logger.info("Start processing LIMS update event")
    logger.info(libjson.dumps(event))

    return persist_lims_data_from_google_drive(
        account_info_ssm_key=os.environ['SSM_KEY_NAME_LIMS_SERVICE_ACCOUNT_JSON'],
        file_id_ssm_key=os.environ['SSM_KEY_NAME_LIMS_SPREADSHEET_ID'],
    )
