import os
import json
import logging
import boto3
import requests
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from gspread_pandas import Spread
from datetime import datetime
import libiap.openapi.libgds
from libiap.openapi.libgds.rest import ApiException
from sample_sheet import SampleSheet  # https://github.com/clintval/sample-sheet

SAMPLE_ID_HEADER = 'Sample_ID (SampleSheet)'
OVERRIDECYCLES_HEADER = 'OverrideCycles'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client("ssm")


def get_secret(key) -> str:
    """
    Retrieve the secret value from SSM.
    :param key: the key of the secret
    :return: the secret value
    """
    resp = client.get_parameter(
        Name=key,
        WithDecryption=True
    )
    return resp['Parameter']['Value']


# TODO: use libssm from utils?
LAB_SHEET_ID = get_secret('/umccr/google/drive/tracking_sheet_id')
SSM_GOOGLE_ACCOUNT_INFO = get_secret('/data_portal/dev/google/lims_service_account_json')
IAP_API_KEY = get_secret('/iap/jwt-token')
IAP_CONF = libiap.openapi.libgds.Configuration(
    host="https://aps2.platform.illumina.com",
    api_key={'Authorization': IAP_API_KEY},
    api_key_prefix={'Authorization': 'Bearer'})


def download_gds_file(gds_volume: str, gds_path: str):
    """Retrieve a GDS file to local (/tmp) storage

    :param gds_path: the GDS path of the file to download
    """
    # TODO: could split into multiple methods
    # call GDS files endpoint to get details (pre-signed URL) of the GDS file
    print(f"Downloading file from GDS - volume:{gds_volume} - path:{gds_path}")
    with libiap.openapi.libgds.ApiClient(IAP_CONF) as api_client:
        api_instance = libiap.openapi.libgds.FilesApi(api_client)
        page_size = 5  # TODO: Should only have one file!
        try:
            api_response = api_instance.list_files(
                volume_name=[gds_volume],
                path=[gds_path],
                page_size=page_size,
                include="presignedUrl")
            # print(f"ListFiles response: {api_response}")
        except ApiException as e:
            print("Exception when calling FilesApi->list_files: %s\n" % e)
    if not api_response:
        raise ValueError("No API response for list files request!")
    # extract pre-signed URL
    if api_response.item_count != 1:
        raise ValueError(f"Unexpected number of files: {api_response.item_count}")
    file_details = api_response.items[0]  # get the FileResponse object
    if file_details.name != 'SampleSheet.csv':
        raise ValueError(f"Got wrong file: {file_details.name}")
    print(f"Pre-signed URL: {file_details.presigned_url}")

    # write the file content to a local path
    r = requests.get(file_details.presigned_url)
    filename = f"/tmp/SampleSheet-{datetime.now().timestamp()}.csv"
    with open(filename, 'wb') as f:
        f.write(r.content)

    return filename


def get_sample_ids_from_samplesheet(path: str):
    samplesheet = SampleSheet(path)
    ids = set()
    for sample in samplesheet:
        ids.add(sample.Sample_ID)
    return ids


def download_metadata(account_info: str, year: int):
    """Download the full original metadata from which to extract the required information

    :param account_info: the Google json file required for accessing the API
    :param year: the sheet in the metadata spreadsheet to load
    """
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    credentials = service_account.Credentials.from_service_account_info(json.loads(account_info))
    spread = Spread(spread=LAB_SHEET_ID, creds=credentials.with_scopes(scopes))
    metadata_df = spread.sheet_to_df(sheet=year, index=0, header_rows=1, start_row=1)
    return metadata_df


def extract_requested_rows(df, requested_ids: list):
    """Extract the metadata for the requested IDs

    :param df: the dataframe containing the full metadata set (a sheet of the spreadsheet)
    :param requested_ids: a list of IDs for which tho extract the metadata entries
    """
    # filter rows by requested ids
    subset_rows = df[df[SAMPLE_ID_HEADER].isin(requested_ids)]
    # filter colums by data needed for workflow
    subset_cols = subset_rows[[SAMPLE_ID_HEADER, OVERRIDECYCLES_HEADER]]
    return subset_cols


def lambda_handler(event, context):
    """
    This lambda would retrieve metadata required for the demultiplexing workflow.
    That is currently metadata from the lab's tracking sheet defining how to group
    samples for demultiplexing. This information will be used to split the initial
    SampleSheet into several, each one with it's own demux options.

    The input to this lambda should be:
    - 'requestedIds': a list of IDs for which to retrieve metadata, in the form used in the SampleSheet
      ('Sample_ID (SampleSheet)' column in the tracking sheet)
    - 'gdsPath': the GDS path to where the metadata file will be written
    """
    # extract list of sample IDs for which to include metadata
    gds_volume = event['gdsVolume']
    gds_base_path = event['gdsBasePath']
    samplesheet_path = event['gdsSamplesheet']
    gds_samplesheet_path = os.path.join(gds_base_path, samplesheet_path)
    print(f"GDS SS path: {gds_samplesheet_path}")
    local_path = download_gds_file(gds_volume=gds_volume, gds_path=gds_samplesheet_path)
    print(f"Local SS path: {local_path}")
    sample_ids = get_sample_ids_from_samplesheet(local_path)
    print(f"Sample IDs: {sample_ids}")

    # download the lab metadata sheets
    df_2019 = download_metadata(account_info=SSM_GOOGLE_ACCOUNT_INFO, year='2019')
    df_2020 = download_metadata(account_info=SSM_GOOGLE_ACCOUNT_INFO, year='2020')
    df_all = df_2019.append(df_2020)

    # extract required records from metadata
    requested_metadata_df = extract_requested_rows(df=df_all, requested_ids=sample_ids)
    print(requested_metadata_df)

    # TODO: turn metadata_df into format compatible with workflow input
    # TODO: wire up with BCL Convert workflow execution
