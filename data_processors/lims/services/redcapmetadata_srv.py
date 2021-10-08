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

def download_redcap_project_data(): #(subjectid_list: dict) -> pd.DataFrame:
    #TODO set up Redcap API key and URL
    #redcap_api_key = libssm.get_secret(const.REDCAP_API_KEY) 
    #redcap_api_url = libssm.get_secret(const.REDCAP_API_URL)
    redcap_api_key = "foo"
    redcap_api_url = "bar"
    project = Project(redcap_api_url, redcap_api_key)
    data_frame = project.export_records(format='df')

    return data_frame


def retrieve_metadata(subjectid_in: list) -> pd.DataFrame:
    """Download the full original metadata from which to extract the required information

    :param year: the sheet in the metadata spreadsheet to load
    """
    df = download_redcap_project_data()
    subjectdf = df.loc[df['subjectid'].isin(subjectid_in)]
    return subjectdf
