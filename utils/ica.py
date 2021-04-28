import logging
import os

from libica.openapi import libgds
from libica.openapi.libgds import FileResponse

from data_processors.pipeline import constant
from utils import libssm

logger = logging.getLogger(__name__)


def configuration(lib):
    ica_access_token = os.getenv("ICA_ACCESS_TOKEN", None)
    if ica_access_token is None:
        ica_access_token = libssm.get_secret(constant.IAP_JWT_TOKEN)
    ica_base_url = os.getenv("ICA_BASE_URL", constant.IAP_BASE_URL)

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
