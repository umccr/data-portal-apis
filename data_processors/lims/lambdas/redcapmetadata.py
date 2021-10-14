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

def handler(event, context): 
    """event payload dict
    {
        'values_in':  { "keyname" : list('the','values') },
        'columns' : ['list','of','cols','to','show']
    }
    Handler for basic RedCap fetching 

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

def handler_pierian_metadata(event, context):
    """event payload dict
    {
        'values_in':  { "keyname" : list('the','values') } 
    }
    Handler for Pierian metadata specific fetching 

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
    

    """ Metadatra needed for Pierian:
    ================
    Sample Type  - default this to Validation Sample
    Indication - sheet says CancerType, not present 
    Disease  - '?'
    Is Identified  study_subcategory
    Requesting Physicians First Name    ?
    Requesting Physicians Last Name ?
    Accession Number    tracking sheet (subectid,libraryid), is this in RedCap
    Patient First Name  n/a
    Patient Last Name   n/a
    Patient Date Of Birth   n/a
    Study ID    ?
    Participant ID  trackingsheet StudyID
    Specimen Type   119361006   
    External Specimen ID    trackingsheet ExternalSubjectID
    Date Accessioned    timestamp of data uplopad to s3 bucket (does this then need to be provided to the lambda call?)
    Date collected  sub_biopsy_date
    Date Received   ?
    Gender  ?
    Ethnicity   ?
    Race    n/a
    Medical Record Numbers  n/a
    Hospital Numbers    ?
    Number of unstable microsatellite loci
    Usable MSI Sites
    Tumor Mutational Burden (Mutations/Mb) n/a
    Percent Unstable Sites n/a
    Percent Tumor Cell Nuclei in the Selected Areas n/a
    """

    col_redcap_pierian_mappings = {"sub_biopsy_date":  "Date collected" } #, "Indication": "cancer_type"  } # this ought to have the field we're searchig on in values_in
    df = redcapmetadata_srv.retrieve_metadata(values_in,col_redcap_pierian_mappings.keys())
    
    # rename cols
    df = df.rename(columns = col_redcap_pierian_mappings)
    
    # add default fields
    df['Sample Type'] = "Validation Sample"
    df['Specimen Type'] = 119361006
    df['Patient First Name'] = 'n/a'
    df['Patient Last Name'] = 'n/a'
    df['Patient Date Of Birth'] = 'n/a'
    df['Race'] = 'n/a'
    df['Medical Record Numbers'] = 'n/a'
    df['Number of unstable microsatellite loci'] = 'n/a'
    df['Usable MSI Sites'] = 'n/a'
    df['Tumor Mutational Burden (Mutations/Mb)'] = 'n/a'
    df['Percent Unstable Sites'] = 'n/a'
    df['Percent Tumor Cell Nuclei in the Selected Areas'] = 'n/a'

    return df