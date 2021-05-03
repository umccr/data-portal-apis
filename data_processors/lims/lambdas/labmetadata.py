try:
    import unzip_requirements
except ImportError:
    pass

import django
import os
import pandas as pd
import re

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
from typing import Dict

from datetime import datetime

from data_processors import const
from data_processors.lims import services
from utils import libjson, libgdrive, libssm

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def scheduled_update_handler(event, context) -> Dict[str, int]:
    """event payload
    {
        "sheet": "2020"
    }
    Handler for LIMS update by reading the designated Spreadsheet file from Google Drive.
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

    year = event.get('sheet', "2021")  # default to 2021

    df = clean_labmetadata_dataframe_columns(download_metadata(year))

    return services.persist_labmetadata(df)


def clean_labmetadata_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    clean a dataframe of labmetadata from a tracking sheet to correspond to the django object model
    we do this by editing the columns to match the django object
    """
    # remove unnamed
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # simplify verbose column names
    df = df.rename(columns={'Coverage (X)': 'coverage', "TruSeq Index, unless stated": "truseqindex"})

    # convert PascalCase headers to snake_case and fix ID going to _i_d
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    df = df.rename(columns=lambda x: pattern.sub('_', x).lower().replace('_i_d', '_id'))

    return df


def download_metadata(year: str) -> pd.DataFrame:
    """Download the full original metadata from which to extract the required information

    :param year: the sheet in the metadata spreadsheet to load
    """
    lab_sheet_id = libssm.get_secret(const.TRACKING_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    return libgdrive.download_sheet(account_info, lab_sheet_id, sheet=year)
