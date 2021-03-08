# Standard imports
import logging
import requests
from tempfile import NamedTemporaryFile
from datetime import datetime

# ICA imports
from libica.openapi import libgds
from libica.openapi.libgds.rest import ApiException

# Utils
from utils import ica

# Get logger
logger = logging.getLogger()


def get_gds_file_id_from_gds_path(volume_name: str, path: str) -> (str, None):
    """
    From a gds path, collect the file id.
    One can then use the file id to collect other metadata on a file
    """

    configuration = libgds.Configuration(
        host="https://aps2.platform.illumina.com",
        api_key={
            'Authorization': 'YOUR_API_KEY'
        }
    )

    with libgds.ApiClient(ica.configuration(libgds)) as api_client:
        api_instance = libgds.FilesApi(api_client)

        try:
            api_response: libgds.FileListResponse = api_instance.list_files(volume_name=[volume_name],
                                                                            path=[path])

        except ApiException as e:
            logger.error("Could not get file id from gds://{}{}".format(volume_name, path))
            return None

    # Check FileResponse list is of length 1
    if not api_response.item_count == 1:
        logger.error("Expected one and only only one item returned from file listing of gds://{}{} but got {}.\n"
                     "Make sure you use this function ONLY for files, not for directories".format(volume_name, path, api_response.item_count))
        return None

    # Collect file id from file response
    file_obj = api_response.items[0]

    # Check file id attribute exists
    if getattr(file_obj, "id", None) is None:
        logger.error("File gds://{}/{} does not have an 'id' attribute".format(volume_name, path))
        return None

    return getattr(file_obj, 'id')


def get_file_obj_from_gds_file_id(file_id: str) -> (libgds.FileResponse, None):
    """
    Retuns a file response object

    :param file_id:
    :return:
    """

    with libgds.ApiClient(ica.configuration(libgds)) as api_client:
        api_instance = libgds.FilesApi(api_client)

        try:
            api_response: libgds.FileResponse = api_instance.get_file(file_id=file_id)

        except ApiException as e:
            logger.error("Could not get file obj from file id".format(file_id))
            return None

    return api_response


def download_gds_file(gds_volume_name: str, gds_path: str) -> (NamedTemporaryFile, None):
    """Retrieve a GDS file

    One must first collect the file object id through a list command (there should be only one item in the list)
    Then collect the presigned url from the file get through the file id.
    Then we use the requests library to download the gds file to temporary storage and outputs a tempfile object.

    The file will be deleted once it goes 'out of scope'.  Use the 'name' output object to read the file

    Call GDS list files endpoint with a filter on given gds_path
    Get details PreSigned URL of the GDS file and write to local /tmp storage

    :param gds_volume_name:
    :param gds_path: the GDS path of the file to download
    :return local_path: or None if file not found
    """

    logger.info(f"Downloading file from GDS: gds://{gds_volume_name}{gds_path}")

    gds_file_id = get_gds_file_id_from_gds_path(volume_name=gds_volume_name,
                                                path=gds_path)

    gds_file_obj = get_file_obj_from_gds_file_id(gds_file_id)

    # Collect presigned url
    if getattr(gds_file_obj, "presigned_url", None) is None:
        logger.error("Could not get the presigned url value "
                     "from the file object from gds://{}/{}".format(gds_volume_name, gds_path))
        return None

    presigned_url = getattr(gds_file_obj, "presigned_url")

    # Download presigned url through requests library
    gds_req = requests.get(presigned_url)

    # Create an temp file
    content_file = NamedTemporaryFile()

    with open(content_file.name, 'wb') as f:
        f.write(gds_req.content)

    return content_file

