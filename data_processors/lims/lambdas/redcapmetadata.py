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

from data_processors.lims.services import redcapmetadata_srv
from utils import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _halt(msg):
    logger.error(msg)
    return {
        'message': msg
    }

#def scheduled_update_handler(event, context):
def handler(event, context):
 
    """event payload dict
    {
        'somekey': ["some", "values"],
        'someboolean': True
    }
    Handler for RedCap update 

    :param event:
    :param context:
    :return: requested redcap metadata
    """


    logger.info("Start processing RedCap datqbaae request update event")
    logger.info(libjson.dumps(event))

    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Fetching data from RedCap at {requested_time}")

    sample_names = event.get('sample_names')
    
    # checks
    if not isinstance(sample_names, list):
        _halt(f"Payload error. Must be array of string for sample_names. Found: {type(sample_names)}")

    #frames = []
    #for year in years:
    #    logger.info(f"Downloading {year} sheet")
    #    frames.append(labmetadata_srv.download_metadata(year))

    return redcapmetadata_srv.retrieve_metadata(sample_names)
