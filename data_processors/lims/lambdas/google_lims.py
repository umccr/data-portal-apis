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
import pandas as pd

from datetime import datetime

from data_processors import const
from data_processors.lims.services import google_lims_srv
from libumccr import libjson, libgdrive
from libumccr.aws import libssm

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _halt(msg):
    logger.error(msg)
    return {
        'message': msg
    }


def scheduled_update_handler(event, context):
    """event payload dict
    {
        'sheets': ["Sheet1", "Sheet2"]
    }

    Handler for LIMS update by reading the designated Spreadsheet file from Google Drive.

    :param event:
    :param context:
    :return: stat of LIMS update
    """
    logger.info("Start processing LIMS update event")
    logger.info(libjson.dumps(event))

    sheets = event.get('sheets', ["Sheet1", ])

    if not isinstance(sheets, list):
        _halt(f"Payload error. Must be array of string for sheets. Found: {type(sheets)}")

    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Reading LIMS data from google drive at {requested_time}")

    lims_sheet_id = libssm.get_secret(const.LIMS_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    frames = []
    for sheet in sheets:
        logger.info(f"Downloading {sheet} sheet")
        frames.append(libgdrive.download_sheet(account_info, lims_sheet_id, sheet))

    df: pd.DataFrame = pd.concat(frames)

    return google_lims_srv.persist_lims_data(df)
