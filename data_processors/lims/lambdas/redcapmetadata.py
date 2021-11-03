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

# mappings for Redcap DB field to Pierian name
# might not need this, if our redcap DB field labels can match exactrly the Pierian fields
REDCAP_TO_PIERIAN_FIELDS = {"sub_biopsy_date":  "Date collected", 
 "subjectid": "Subject ID", 
 "snomed_code": "Disease",
 "study_subcategory": "Is Identified?",
 "study_name": "Study ID",
 "sub_biopsy_date": "Date Collected",
 "tdna_receivedate": "Date Received",
 "enr_patient_sex": "Gender",
 "enr_patient_ethnicity": "Ethnicity",
 "cancer_type": "Indication"
} 

def _halt(msg):
    logger.error(msg)
    return {
        'message': msg
    }



    values_in = { 'subjectid' : [d.subject_id for d in lms] } 
def handler(event, context):
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
            _halt(f"Payload error. Must be list of strings. found list of: {type(k)}")
    
    lms = LabMetadata.objects.filter(library_id__in=library_ids)
    if not lms:
        return pd.DataFrame({})

    values_in = { 'subjectid' : [d.subject_id for d in lms] }  # will search for these in redcap
    df_lab = pd.DataFrame( { 'subject_id' : [d.subject_id for d in lms] , 'library_id' : [d.library_id for d in lms] } )  # TODO check if iteration order always the same, ELSE transfer to preserved-order structure

    # dict with keys as redcap fields to retrieve, values as pieran fields to translate and output
    col_redcap_pierian_mappings = REDCAP_TO_PIERIAN_FIELDS #,"enr_patient_enrol_date":"enr_patient_enrol_date","createdate":"createdate","req_request_date":"req_request_date","report_date":"report_date","req_diagnosis":"req_diagnosis" }

    #if event.get('source') and event.get('source') == 'googlesheet':
    #    df_redcap = redcapmetadata_srv.retrieve_metadata_googlesheet(values_in,col_redcap_pierian_mappings.keys()) 
    #else:
    #    df_redcap = redcapmetadata_srv.retrieve_metadata(values_in,col_redcap_pierian_mappings.keys()) 
    cols = list(col_redcap_pierian_mappings.keys())
    df_redcap = redcapmetadata_srv.retrieve_metadata(values_in,cols) 
    if df_redcap.empty:
        logger.warning("Empty dataframe came back from metadata retrieval")
        return df_redcap

    # merge labmetadata and redcap frames
    df_redcap = df_redcap.rename(columns = { "subjectid": "subject_id" })
    df = df_lab.merge(df_redcap, on='subject_id', how='right')  

    col_redcap_pierian_mappings["library_id"] =  "Library ID" 
    col_redcap_pierian_mappings["subject_id"] =  "Participant ID" 
    df = df.rename(columns = col_redcap_pierian_mappings)

    # add default fields
    df['Sample Type'] = "Validation Sample"
    df['Specimen Type'] = 119361006
    df['Patient First Name'] = 'n/a'
    df['Patient Last Name'] = 'n/a'
    df['Patient Date Of Birth'] = 'n/a'
    df['Requesting Physician First Name'] = 'n/a'
    df['Requesting Physician Last Name'] = 'n/a'
    df['Race'] = 'n/a'
    df['Is Identified?'] = "False"
    df['Medical Record Numbers'] = 'n/a'
    df['Number of unstable microsatellite loci'] = 'n/a'
    df['Usable MSI Sites'] = 'n/a'
    df['Tumor Mutational Burden (Mutations/Mb)'] = 'n/a'
    df['Percent Unstable Sites'] = 'n/a'
    df['Percent Tumor Cell Nuclei in the Selected Areas'] = 'n/a'
    df['Participant ID'] = df["Participant ID"]
    df['Accession Number'] = df["Participant ID"] + "_" + df["Library ID"]
    df['External Specimen ID'] = df.apply (lambda row: lms.filter(library_id__iexact=row["Library ID"])[0].external_subject_id, axis=1)
    df['Indication'] = "CancerType"

    date_accessioned = event.get('date_accessioned')
    if date_accessioned:
        df['Date Accessioned'] = date_accessioned
    else:
        df['Date Accessioned'] = 'n/a'

    # drop subject and librardy ID fields ?
    return df
