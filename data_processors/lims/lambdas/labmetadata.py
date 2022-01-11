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
from datetime import datetime

import pandas as pd

from data_processors.lims.services import labmetadata_srv
from libumccr import libjson

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
        'sheets': ["2020", "2021", "2022"],
        'truncate': True
    }
    Handler for LabMetadata update by reading the designated Spreadsheet file from Google Drive.
    Can be hit by Cron like job scheduler. It uses EventBridge on default EventBus with cron(0 12 * * ? *). See REF.

    REF:
    https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html
    https://docs.aws.amazon.com/eventbridge/latest/userguide/event-types.html#schedule-event-type
    https://docs.aws.amazon.com/eventbridge/latest/userguide/run-lambda-schedule.html

    :param event:
    :param context:
    :return: dict of update counters
    """
    logger.info("Start processing LabMetadata update event")
    logger.info(libjson.dumps(event))

    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Reading LabMetadata sheet from google drive at {requested_time}")

    years = event.get('sheets', ["2020", "2021", "2022"])
    is_truncate = event.get('truncate', True)

    if not isinstance(years, list):
        _halt(f"Payload error. Must be array of string for sheets. Found: {type(years)}")

    if not isinstance(is_truncate, bool):
        _halt(f"Payload error. Must be boolean for truncate. Found: {type(is_truncate)}")

    truncated = False
    if is_truncate:
        truncated = labmetadata_srv.truncate_labmetadata()

    if not truncated:
        logger.warning(f"LabMetadata table is not truncated. Continue with create or update merging strategy.")
        # Note we can decide to error out and halt here instead

    frames = []
    for year in years:
        logger.info(f"Downloading {year} sheet")
        frames.append(labmetadata_srv.download_metadata(year))

    df = pd.concat(frames)

    return labmetadata_srv.persist_labmetadata(df)
