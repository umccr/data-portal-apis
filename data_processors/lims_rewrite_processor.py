import os, django
# We need to set up django app first
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# All other imports should be placed below
import logging
from typing import Dict
from data_processors.persist_lims_data import persist_lims_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context) -> Dict[str, int]:
    """
    Handler for LIMS rewrite triggering
    :return: result of LIMS rewrite
    """
    return persist_lims_data(
        csv_bucket=os.environ['LIMS_BUCKET_NAME'],
        csv_key=os.environ['LIMS_CSV_OBJECT_KEY'],
        rewrite=True,
        create_association=False,
    )
