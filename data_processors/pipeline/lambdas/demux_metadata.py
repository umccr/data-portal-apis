try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import json
import logging
from datetime import datetime

import requests
import pandas as pd
import gspread
from google.oauth2 import service_account
from gspread_pandas import Spread
from libiap.openapi import libgds
from sample_sheet import SampleSheet

from data_processors.pipeline import services, constant
from data_processors.pipeline.constant import SampleSheetCSV
from utils import libssm, libjson, iap

SAMPLE_ID_HEADER = 'Sample_ID (SampleSheet)'
OVERRIDECYCLES_HEADER = 'OverrideCycles'

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def download_gds_file(gds_volume: str, gds_path: str):
    """Retrieve a GDS file

    Call GDS list files endpoint with a filter on given gds_path
    Get details PreSigned URL of the GDS file and write to local /tmp storage

    :param gds_volume:
    :param gds_path: the GDS path of the file to download
    :return local_path: or None if file not found
    """

    logger.info(f"Downloading file from GDS: gds://{gds_volume}{gds_path}")

    local_path = None
    file_details = None

    with libgds.ApiClient(iap.configuration(libgds)) as api_client:
        file_api = libgds.FilesApi(api_client)
        try:
            page_token = None
            while True:
                file_list: libgds.FileListResponse = file_api.list_files(
                    volume_name=[gds_volume],
                    path=[gds_path],
                    page_size=1000,
                    page_token=page_token,
                )

                for item in file_list.items:
                    file: libgds.FileResponse = item
                    if file.name == SampleSheetCSV.FILENAME.value:
                        file_details = file_api.get_file(file.id)
                        file_list.next_page_token = None
                        break

                page_token = file_list.next_page_token
                if not file_list.next_page_token:
                    break
            # while end

        except libgds.ApiException as e:
            logger.info("Exception when calling FilesApi: %s\n" % e)

    if file_details:
        logger.info(f"PreSigned URL: {file_details.presigned_url}")
        req = requests.get(file_details.presigned_url)
        local_path = f"/tmp/SampleSheet-{datetime.now().timestamp()}.csv"
        with open(local_path, 'wb') as f:
            f.write(req.content)

    return local_path


def get_sample_ids_from_samplesheet(path: str):
    samplesheet = SampleSheet(path)
    ids = set()
    for sample in samplesheet:
        ids.add(sample.Sample_ID)
    return ids


def download_metadata(year: str) -> pd.DataFrame:
    """Download the full original metadata from which to extract the required information

    :param year: the sheet in the metadata spreadsheet to load
    """
    lab_sheet_id = libssm.get_secret(constant.TRACKING_SHEET_ID)
    account_info = libssm.get_secret(constant.GDRIVE_SERVICE_ACCOUNT)

    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    credentials = service_account.Credentials.from_service_account_info(json.loads(account_info))
    spread = Spread(spread=lab_sheet_id, creds=credentials.with_scopes(scopes))

    try:
        return spread.sheet_to_df(sheet=year, index=0, header_rows=1, start_row=1)
    except (gspread.exceptions.WorksheetNotFound, gspread.exceptions.APIError) as e:
        logger.warning(f"Returning empty data frame for sheet {year}. Exception: {type(e).__name__} -- {e}")
        return pd.DataFrame()


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


def handler(event, context):
    """event payload dict
    {
        'gdsVolume': "bssh.xxxx",
        'gdsBasePath': "/Runs/ccc.aaa"
        'gdsSamplesheet': "SampleSheet.csv"
    }

    This lambda would retrieve metadata required for the demultiplexing workflow.
    That is currently metadata from the lab's tracking sheet defining how to group
    samples for demultiplexing. This information will be used to split the initial
    SampleSheet into several, each one with it's own demux options.

    :param event:
    :param context:
    :return: dict contains metadata
    """
    logger.info(f"Start processing demux_metadata event")
    logger.info(libjson.dumps(event))

    # extract list of sample IDs for which to include metadata
    gds_volume = event['gdsVolume']
    gds_base_path = event['gdsBasePath']
    samplesheet_path = event['gdsSamplesheet']

    gds_samplesheet_path = os.path.join(gds_base_path, samplesheet_path)
    logger.info(f"GDS sample sheet path: {gds_samplesheet_path}")

    local_path = download_gds_file(gds_volume=gds_volume, gds_path=gds_samplesheet_path)
    if local_path is None:
        reason = f"Abort extracting metadata process. " \
                f"Can not download sample sheet from GDS: gds://{gds_volume}{gds_samplesheet_path}"
        abort_message = {'message': reason}
        logger.warning(libjson.dumps(abort_message))
        services.notify_outlier(topic="Sample sheet download issue", reason=reason, status="Aborted", event=event)
        return abort_message

    logger.info(f"Local sample sheet path: {local_path}")
    sample_ids = get_sample_ids_from_samplesheet(local_path)
    logger.info(f"Sample IDs: {sample_ids}")

    # download the lab metadata sheets
    df_2019 = download_metadata(year="2019")
    df_2020 = download_metadata(year="2020")
    df_2021 = download_metadata(year="2021")
    df_all = df_2019.append(df_2020.append(df_2021))

    # extract required records from metadata
    requested_metadata_df = extract_requested_rows(df=df_all, requested_ids=list(sample_ids))
    if requested_metadata_df.empty:
        logger.warning(f"Can not extract any associated metadata tracking for sample sheet: "
                       f"gds://{gds_volume}{gds_samplesheet_path}")
    else:
        logger.info(f"REQUESTED_METADATA_DF: \n{requested_metadata_df}")

    # turn metadata_df into format compatible with workflow input
    sample_array = requested_metadata_df[SAMPLE_ID_HEADER].values.tolist()
    orc_array = requested_metadata_df[OVERRIDECYCLES_HEADER].values.tolist()

    return {
        'samples': sample_array,
        'override_cycles': orc_array,
    }
