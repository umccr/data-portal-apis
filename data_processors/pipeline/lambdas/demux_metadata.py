import os
import json
import logging
import boto3
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from gspread_pandas import Spread
from datetime import datetime
import libiap.openapi.libgds
from libiap.openapi.libgds.rest import ApiException
from utils import libssm

# TODO: sort out env variables
# TODO: sort out SSM parameters/secrets

SAMPLE_ID_HEADER = 'Sample_ID (SampleSheet)'
LAB_SHEET_ID = os.environ.get('LAB_SHEET_ID')  # TODO: maybe get from SSM ParameterStore?
SSM_KEY_GOOGLE_ACCOUNT_INFO = os.environ.get('SSM_KEY_GOOGLE_ACCOUNT_INFO')
IAP_API_KEY = os.environ.get('IAP_API_KEY')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def upload_file(client, file_name: str, bucket: str, object_name: str):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name
    :return: True if file was uploaded, else False
    """

    try:
        client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True


def get_gds_upload_details(gds_path: str):
    """Create a file placeholder in GDS and get S3 upload credentials

    :param gds_path: the GDS path where to create file resource
    """
    # call GDS create file endpoint and extract details for file upload
    api_key = libssm.get_secret(IAP_API_KEY)
    configuration = libiap.openapi.libgds.Configuration(
        host="https://aps2.platform.illumina.com",
        api_key={'Authorization': api_key})
    configuration.api_key_prefix['Authorization'] = 'Bearer'

    with libiap.openapi.libgds.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = libiap.openapi.libgds.FilesApi(api_client)
        body = libiap.openapi.libgds.CreateFileRequest()
        include = 'objectStoreAccess'
        try:
            # Create a file entry in GDS and get temporary credentials for upload
            api_response = api_instance.create_file(body, include=include)
        except ApiException as e:
            print("Exception when calling FilesApi->create_file: %s\n" % e)

    # parse API response
    aws_creds = api_response['objectStoreAccess']['awsS3TemporaryUploadCredentials']
    aws_access_key = aws_creds['access_Key_Id']
    aws_secret_key = aws_creds['secret_Access_Key']
    aws_session_token = aws_creds['session_Token']
    aws_bucket = aws_creds['bucketName']
    aws_path = aws_creds['keyPrefix']

    return aws_access_key, aws_secret_key, aws_session_token, aws_bucket, aws_path


def get_gds_base_path(event):
    # TODO: only placeholder, need to figure out what the event contains
    # parse event to get the GDS base path (where to write the metadata file)
    return event['gdsPath']


def get_requested_ids(event):
    # TODO: only placeholder, need to figure out what the event contains
    # parse event to retrieve requested IDs (parsed from the SampleSheet)
    return event['requestedIds']


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
    subset = df[df[SAMPLE_ID_HEADER].isin(requested_ids)]
    # TODO: restrict to only needed columns
    # TODO: future improvement: define metadata format for workflow (independent of metadata spreadsheet)
    return subset


def write_to_gds(df, gds_path: str):
    """Write the metadata DF as CSV to GDS

    This first writes a CSV file with the metadata to a local space (/tmp). Then creates a GDS
    file placeholder and AWS access credentials to populate it, and finally upload the local 
    file to S3 (backing GDS).

    :param df: the metadata DataFrame to write
    :param gds_path: the GDS location where to write the metadata file to
    """
    # TODO: that requires a samplesheet split script change, as that expects metadata in Excel format
    # write DF to (Lambda) local temp file
    filename = f"/tmp/metadata-{datetime.now()}.csv"
    df.to_csv(filename, index=False)

    # upload local file to GDS
    aws_access_key, aws_secret_key, aws_session_token, aws_bucket, aws_path = get_gds_upload_details(gds_path)
    s3_client = boto3.client(
        service_name='s3',
        region_name='ap-southeast-2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        aws_session_token=aws_session_token
    )
    upload_file(client=s3_client, file_name=filename, bucket=aws_bucket, object_name=aws_path)

    # remove local file
    os.remove(filename)


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
    # get Google credentials from secrets store
    google_account_info = libssm.get_secret(SSM_KEY_GOOGLE_ACCOUNT_INFO)

    # extract list of sample IDs for which to include metadata
    requested_ids = get_requested_ids(event)
    # extract the GDS upload path for the metadata file
    gds_base_path = get_gds_base_path(event)

    # download the metadata
    # TODO: extend to cover multiple years
    df_2019 = download_metadata(account_info=google_account_info, year='2019')

    # extract required records from metadata
    # TODO: cover multiple years
    requested_metadata_df = extract_requested_rows(df=df_2019, requested_ids=requested_ids)

    # write metadata to GDS
    write_to_gds(df=requested_metadata_df, gds_path=gds_base_path)
