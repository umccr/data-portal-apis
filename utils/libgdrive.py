# -*- coding: utf-8 -*-
"""libgdrive module

Module interface for underlay Google Drive client operations
Loosely based on design patterns: Facade, Adapter/Wrapper

Should retain/suppress all Google Drive API calls here, including
Google API specific exceptions and data type that need for processing response.

Goal is, so that else where in code, it does not need to depends on googleapiclient
API directly. i.e. No more import googleapiclient, but just import libgdrive instead.

If unsure, start with Pass-through call.
"""
import io
import json
import logging

import gspread
import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account
from gspread_pandas import Spread

logger = logging.getLogger()

_scopes = ['https://www.googleapis.com/auth/drive.readonly']


def download_sheet1_csv(account_info: str, file_id: str) -> bytes:
    """
    Get a Google drive Spreadsheet file Sheet1 as exported in CSV format
    Required a Service Account with Read-Only permission on the requested file resource

    Limitation: Only able to grab the first Sheet

    REF:
    https://developers.google.com/drive/api/v3/manage-downloads
    https://google-auth.readthedocs.io/en/latest/user-guide.html

    :param account_info:
    :param file_id:
    :return file content: in csv format bytes
    """

    credentials = service_account.Credentials.from_service_account_info(json.loads(account_info))
    service = build('drive', 'v3', credentials=credentials.with_scopes(_scopes), cache_discovery=False)
    request = service.files().export_media(fileId=file_id, mimeType='text/csv')

    fh = io.BytesIO()  # Create temporary in memory file handler
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    logger.info(f"Downloaded spreadsheet from google drive")

    content = fh.getvalue()
    fh.close()
    return content


def download_sheet(account_info: str, file_id: str, sheet=None) -> pd.DataFrame:
    """Download the specified sheet from GDrive and return as panda DataFrame object

    :param account_info:
    :param file_id:
    :param sheet: str,int the sheet in the metadata spreadsheet to load
    :return dataframe: file content in panda dataframe, NOTE: it still return blank DF if exception occur
    """

    credentials = service_account.Credentials.from_service_account_info(json.loads(account_info))
    spread = Spread(spread=file_id, creds=credentials.with_scopes(_scopes))

    try:
        return spread.sheet_to_df(sheet=sheet, index=0, header_rows=1, start_row=1)
    except (gspread.exceptions.WorksheetNotFound, gspread.exceptions.APIError) as e:
        logger.warning(f"Returning empty data frame for sheet {sheet}. Exception: {type(e).__name__} -- {e}")
        return pd.DataFrame()
