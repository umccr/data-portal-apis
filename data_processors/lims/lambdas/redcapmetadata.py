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
from data_portal.models import LabMetadata

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#TODO we might want to rename this lambda e.g. not recapmetadata but pierianmetadata

def _halt(msg):
    logger.error(msg)
    return {
        'message': msg
    }


def handler_pierian_metadata_by_library_id(event, context):
    """
    given a list of library IDs, get their subject IDs from LabMetadata & fetch RedCap info needed for Pieran DX

    event payload dict
    {
        library_ids = ['some','library','ids']
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

    library_ids = event.get('library_ids')

    if not isinstance(library_ids, list):
        _halt(f"Payload error. Must be list. Found: {type(list)}")

    for k in library_ids:
        if not isinstance(k, str):
            _halt(f"Payload error. Must be list of strings. found lit of: {type(k)}")
    
    lms = LabMetadata.objects.filter(library_id__in=library_ids)
    if not lms:
        return pd.DataFrame({})

    values_in = { 'subjectid' : [d.subject_id for d in lms] }   # will search for these in redcap
    df = pd.DataFrame( { 'subject_id' : [d.subject_id for d in lms] , 'library_id' : [d.library_id for d in lms] } )  # TODO check if iteration order always the same, ELSE transfer to preserved-order structure

    #   grab neccessary columns from redcap as a df, merge them in
    col_redcap_pierian_mappings = {"sub_biopsy_date":  "Date collected", "subjectid": "Subject Id","enr_patient_enrol_date":"enr_patient_enrol_date","createdate":"createdate","req_request_date":"req_request_date","report_date":"report_date","req_diagnosis":"req_diagnosis" }
    df_redcap = redcapmetadata_srv.retrieve_metadata(values_in,col_redcap_pierian_mappings.keys())
    df_redcap = df_redcap.rename(columns = { "subjectid": "subject_id" })
    df = df.merge(df_redcap, on='subject_id', how='right')

    # rename cols to Pieran DX sheet names TODO figure these out, also consolifate
    df = df.rename(columns = col_redcap_pierian_mappings)   
    df = df.rename(columns = { "library_id": "Library Id" , "subject_id": "Subject Id" })
    
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
    df['Participant ID'] = df["Subject Id"]
    df['Accession Number'] = df["Subject Id"] + "_" + df["Library Id"]
    df['External Specimen ID'] = df.apply (lambda row: lms.filter(library_id__iexact=row["Library Id"])[0].external_subject_id, axis=1)

    #TODO drop subject and librardy ID fields ?
    return df


def handler(event, context): 
    # basic handler for generic redcap retriecal, not actually used 
    ### TODO delete?
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


def handler_pierian_metadata_only(event, context):
    # slightly more complex handler , pieran specific, but no labmetadata retrieval
    ### TODO delete?

    """
    grabs metadata from pieran but ignored labmetadata 
    event payload dict
    {
        'values_in':  { "keyname" : list('the','values') } 
        #subject_library_ids : [ { "subject_id": "xxx", "library_id": "xxx" }, { "subject_id": "xxx", "library_id": "xxx" } ]
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
