try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import io
import logging
from typing import Dict

from datetime import datetime

from data_processors import const
from data_processors.lims.services import google_lims_srv
from utils import libjson, libgdrive, libssm

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

    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Reading LIMS data from google drive at {requested_time}")

    lims_sheet_id = libssm.get_secret(const.LIMS_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    bytes_data = libgdrive.download_sheet1_csv(account_info, lims_sheet_id)

    return google_lims_srv.persist_lims_data(io.BytesIO(bytes_data))
