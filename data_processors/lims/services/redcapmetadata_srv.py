import logging
import re

import numpy as np
import pandas as pd
from django.db import transaction

from data_portal.models import LabMetadata
from data_processors import const
from utils import libssm, libgdrive, libjson
from redcap import Project

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def connect_redcap(redcap_api_key,redcap_api_url):
    return Project(redcap_api_url, redcap_api_key)

def retrieve_metadata(sample_names_in: dict) -> pd.DataFrame:
    """Download the full original metadata from which to extract the required information

    :param year: the sheet in the metadata spreadsheet to load
    """
    #redcap_api_key = libssm.get_secret(const.REDCAP_API_KEY) 
    #redcap_api_url = libssm.get_secret(const.REDCAP_API_URL)
    redcap_api_key = "foo"
    redcap_api_url = "bar"
    project = connect_redcap(redcap_api_key,redcap_api_url)

    


    # filter by sample_names_in, or None for all
    if (not sample_names_in):
        return project.export_records(format='df')
    else:
        data_frame = project.export_records(format='df')

    return data_frame
