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
        'subjectid_in': ["subject1", "subject2"],
        #TODO could make it a dict: 'values_in':  { "keyname" : list('the','values') } ??? 
    }
    Handler for RedCap fetching 

    :param event:
    :param context:
    :return: dataframe requested redcap metadata
    """

    logger.info("Start processing RedCap database request update event")
    logger.info(libjson.dumps(event))

    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Fetching data from RedCap at {requested_time}")

    values_in = event.get('values_in')
    if not isinstance(values_in, dict):
        _halt(f"Payload error. Must be dict. Found: {type(values_in)}")
    for k,v in values_in.items():
        if not isinstance(k, str):
            _halt(f"Payload error. Must be dict with str keys and list values. Key found: {type(k)}")
        if not isinstance(v, list):
            _halt(f"Payload error. Must be dict with str keys and list values. Value found: {type(v)}")
    columns = event.get('columns')
    if not columns:
        columns = list()
    return redcapmetadata_srv.retrieve_metadata(values_in,columns)