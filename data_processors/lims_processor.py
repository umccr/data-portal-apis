try:
  import unzip_requirements
except ImportError:
  pass

import os, django
# We need to set up django app first
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# All other imports should be placed below
import logging
from typing import Dict

from data_processors.services import persist_lims_data, persist_lims_data_from_google_drive

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_handler(event, context) -> Dict[str, int]:
    """
    Update handler for LIMS data csv file drop into designated S3 bucket

    :param event:
    :param context:
    :return: stat of LIMS update
    """
    return persist_lims_data(
        csv_bucket=os.environ['LIMS_BUCKET_NAME'],
        csv_key=os.environ['LIMS_CSV_OBJECT_KEY'],
        rewrite=False,
        create_association=False,
    )


def rewrite_handler(event, context) -> Dict[str, int]:
    """
    Update handler for LIMS data csv file drop into designated S3 bucket with rewrite True

    :param event:
    :param context:
    :return: stat of LIMS update
    """
    return persist_lims_data(
        csv_bucket=os.environ['LIMS_BUCKET_NAME'],
        csv_key=os.environ['LIMS_CSV_OBJECT_KEY'],
        rewrite=True,
        create_association=False,
    )


def scheduled_update_handler(event, context) -> Dict[str, int]:
    """
    Update handler for LIMS data by reading the designated Spreadsheet file from Google Drive
    Design to be hit by Cron like job event scheduler

    :param event:
    :param context:
    :return: stat of LIMS update
    """
    return persist_lims_data_from_google_drive(
        account_info_ssm_key=os.environ['SSM_KEY_NAME_LIMS_SERVICE_ACCOUNT_JSON'],
        file_id_ssm_key=os.environ['SSM_KEY_NAME_LIMS_SPREADSHEET_ID'],
    )
