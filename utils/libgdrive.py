# -*- coding: utf-8 -*-
"""libgdrive module

Module interface for underlay Google Drive and Sheet common operations
"""
import json
import logging

import gspread
import pandas as pd
from google.oauth2 import service_account
from gspread_pandas import Spread

logger = logging.getLogger(__name__)

_scopes = ['https://www.googleapis.com/auth/drive.readonly']


def download_sheet1_csv(account_info: str, file_id: str) -> bytes:
    """
    Get a Google drive Spreadsheet file Sheet1 as exported in CSV format
    Required a Service Account with Read-Only permission on the requested file resource

    Limitation: Only first Sheet

    :param account_info:
    :param file_id:
    :return file content: in csv format bytes
    """

    gc = gspread.service_account_from_dict(json.loads(account_info), scopes=_scopes)
    sh: gspread.Spreadsheet = gc.open_by_key(file_id)

    return pd.DataFrame(sh.sheet1.get_all_records()).to_csv().encode()


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
