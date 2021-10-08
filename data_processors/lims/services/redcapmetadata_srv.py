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
    """Download metadata from redcap database

    :param subjectid_in: list of subjectids to query
    """
    df = download_redcap_project_data()
    
    # we want: date-accessioned, date-modified and date-created, specimen id / label and disease id / label
    # TODO find out what columns match this
    #df = df[["subjectid","enr_patient_enrol_date","createdate","req_request_date","report_date","req_diagnosis"]]
    
    subjectdf = df.loc[df['subjectid'].isin(subjectid_in)]
    return subjectdf
