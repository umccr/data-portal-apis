import logging
import os

from libiap.openapi import libgds
from libiap.openapi.libgds import FileResponse

from data_processors.pipeline import constant
from utils import libssm

logger = logging.getLogger(__name__)


def configuration(lib):
    iap_auth_token = os.getenv("IAP_AUTH_TOKEN", None)
    if iap_auth_token is None:
        iap_auth_token = libssm.get_secret(constant.IAP_JWT_TOKEN)
    iap_base_url = os.getenv("IAP_BASE_URL", constant.IAP_BASE_URL)

    config = lib.Configuration(
        host=iap_base_url,
        api_key={
            'Authorization': iap_auth_token
        },
        api_key_prefix={
            'Authorization': "Bearer"
        },
    )

    # WARNING: only in local debug purpose, should never be committed uncommented!
    # it print stdout all libiap.openapi http calls activity including JWT token in http header
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
