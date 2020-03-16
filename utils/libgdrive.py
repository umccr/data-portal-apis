import io
import json
import logging

from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account

logger = logging.getLogger()


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
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    credentials = service_account.Credentials.from_service_account_info(json.loads(account_info))
    service = build('drive', 'v3', credentials=credentials.with_scopes(scopes), cache_discovery=False)
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
