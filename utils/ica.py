import logging
import os
from enum import Enum

from libica.openapi import libgds
from libica.openapi.libgds import FileResponse

from utils import libssm

logger = logging.getLogger(__name__)

ICA_BASE_URL = "https://aps2.platform.illumina.com"
ICA_JWT_TOKEN = "/iap/jwt-token"


class ENSEventType(Enum):
    """
    REF:
    https://support-docs.illumina.com/SW/ICA/Content/SW/ICA/ENS_AvailableEvents.htm
    """
    GDS_FILES = "gds.files"
    BSSH_RUNS = "bssh.runs"
    WES_RUNS = "wes.runs"


class GDSFilesEventType(Enum):
    """
    REF:
    https://support-docs.illumina.com/SW/ICA/Content/SW/ICA/ENS_AvailableEvents.htm
    """
    UPLOADED = "uploaded"
    DELETED = "deleted"
    ARCHIVED = "archived"
    UNARCHIVED = "unarchived"

    @classmethod
    def from_value(cls, value):
        if value == cls.UPLOADED.value:
            return cls.UPLOADED
        elif value == cls.DELETED.value:
            return cls.DELETED
        elif value == cls.ARCHIVED.value:
            return cls.ARCHIVED
        elif value == cls.UNARCHIVED.value:
            return cls.UNARCHIVED
        else:
            raise ValueError(f"No matching enum found for value: {value}")


def configuration(lib):
    ica_access_token = os.getenv("ICA_ACCESS_TOKEN", None)
    if ica_access_token is None:
        ica_access_token = libssm.get_secret(ICA_JWT_TOKEN)
    ica_base_url = os.getenv("ICA_BASE_URL", ICA_BASE_URL)

    config = lib.Configuration(
        host=ica_base_url,
        api_key={
            'Authorization': ica_access_token
        },
        api_key_prefix={
            'Authorization': "Bearer"
        },
    )

    # WARNING: only in local debug purpose, should never be committed uncommented!
    # it print stdout all libica.openapi http calls activity including JWT token in http header
    # config.debug = True

    return config


def presign_gds_file(file_id: str, volume_name: str, path_: str) -> (bool, str):
    with libgds.ApiClient(configuration(libgds)) as gds_client:
        files_api = libgds.FilesApi(gds_client)
        try:
            file_details: FileResponse = files_api.get_file(file_id=file_id)
            return True, file_details.presigned_url
        except libgds.ApiException as e:
            message = f"Failed to sign the specified GDS file (gds://{volume_name}{path_}). Exception - {e}"
            logger.error(message)
            return False, message
