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

#TODO RENAME CLINICALMETADAT SERVICE?

def download_redcap_project_data(fields_of_interest: list): #(subjectid_list: dict) -> pd.DataFrame:
    #TODO set up Redcap API key and URL
    redcap_api_key = libssm.get_secret(const.REDCAP_API_KEY) 
    redcap_api_url = libssm.get_secret(const.REDCAP_API_URL)   #'https://redcap.healthinformatics.unimelb.edu.au/api/'
    
    project = Project(redcap_api_url, redcap_api_key)
    if(fields_of_interest):
        data_frame = project.export_records(format='df',fields=fields_of_interest)
    else:
        data_frame = project.export_records(format='df')
    return data_frame
 
def retrieve_metadata(values_in: dict, columns: list) -> pd.DataFrame:
    """Download metadata from redcap database

    :param values_in: dict where key is columnnames and value is list of values to match on: will return entries where columnanme is in value list
    :param columns: list of columns to return, or None/empty for all
    """
    df = download_redcap_project_data(columns)

    for colname,list_isin in values_in.items():
        df = df.loc[df[colname].isin(list_isin)] 
    if columns:
        df = df[columns]
    return df

def download_googlesheet_project_data():
    #lab_sheet_id = libssm.get_secret(const.METADATA_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)
    lab_sheet_id = '1wqmlEp04ux47b2efBE3CH3Oq3_yej2fR2V0n2zEENL4' #'486263716'
    #ACCOUNTINFO="'"
    #lab_sheet_id = libssm.get_secret(LAB_METADATA_SHEET_ID)
    #account_info = libssm.get_secret(account_info)

    return libgdrive.download_sheet(account_info, lab_sheet_id)

def retrieve_metadata_googlesheet(values_in: dict, columns: list) -> pd.DataFrame:
    """Download metadata from redcap database

    :param values_in: dict where key is columnnames and value is list of values to match on: will return entries where columnanme is in value list
    :param columns: list of columns to return, or None/empty for all
    """
    df = download_googlesheet_project_data()

    for colname,list_isin in values_in.items():
        df = df.loc[df[colname].isin(list_isin)] 
    if columns:
        df = df[columns]
    return df