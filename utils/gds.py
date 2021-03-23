# Standard imports
import logging
import requests
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse


# ICA imports
from libica.openapi import libgds
from libica.openapi.libgds.rest import ApiException

# Utils
from utils import ica

# Get logger
logger = logging.getLogger()


def check_gds_file(gds_path: str) -> None:
    """
    Check gds path exists, raise error if otherwise
    :param gds_path:
    """

    # Extract parts
    volume_name, path_ = parse_gds_path(gds_path)

    # Verify file exists  # TODO try - catch etc
    get_gds_file_list(volume_name=volume_name, path=path_)


def parse_gds_path(gds_path):
    gds_url_obj = urlparse(gds_path)
    return gds_url_obj.netloc, gds_url_obj.path


def get_volume_name_from_volume_id(volume_id) -> (str, None):
    """
    Get volume object from the volume id
    :param str volume_id: The id of the volume
    :return:
    """

    # Enter a context with an instance of the API client
    with libgds.ApiClient(ica.configuration(libgds)) as api_client:
        # Create an instance of the API class
        api_instance = libgds.VolumesApi(api_client)

    try:
        # Get information for the specified volume ID or volume name
        api_response = api_instance.get_volume(volume_id)
    except ApiException as e:
        logger.error("Exception when calling VolumesApi->get_volume: %s\n" % e)
        return None  # FIXME

    # Check name is valid
    if getattr(api_response, "name", None) is None:
        return None  # FIXME

    return getattr(api_response, "name")


def get_gds_file_list(**kwargs) -> (str, None):
    """
    From a gds path, collect the file id.
    One can then use the file id to collect other metadata on a file
    list_files(self, **kwargs):  # noqa: E501

    Given a volumeId or volume name, get a list of files accessible by the JWT. The default sort returned is alphabetical, ascending. The default page size is 10 items  # noqa: E501
    This method makes a synchronous HTTP request by default. To make an
    asynchronous HTTP request, please pass async_req=True
    # >>> thread = api.list_files(async_req=True)
    # >>> result = thread.get()

    :param async_req bool: execute request asynchronously
    :param str volume_id: Optional field that specifies comma-separated volume IDs to include in the list
    :param str volume_name: Optional field that specifies comma-separated volume names to include in the list
    :param str path: Optional field that specifies comma-separated paths to include in the list. Value can use wildcards (e.g. /a/b/c/*) or exact matches (e.g. /a/b/c/d/).
    :param bool is_uploaded: Optional field to filter by Uploaded files
    :param str archive_status: Optional field that specifies comma-separated Archive Statuses to include in the list
    :param bool recursive: Optional field to specify if files should be returned recursively in and under the specified paths, or only directly in the specified paths
    :param int page_size: START_DESC END_DESC
    :param str page_token: START_DESC END_DESC
    :param str include: START_DESC END_DESC
    :param str tenant_id: Optional parameter to see shared data in another tenant
    :param _preload_content: if False, the urllib3.HTTPResponse object will
                             be returned without reading/decoding response
                             data. Default is True.
    :param _request_timeout: timeout setting for this request. If one
                             number provided, it will be total request
                             timeout. It can also be a pair (tuple) of
                             (connection, read) timeouts.
    :return: FileListResponse
             If the method is called asynchronously,
             returns the request thread.
    """

    # Handle kwargs
    # Check volume id / volume name is legit
    if "volume_id" not in kwargs.keys() and "volume_name" not in kwargs.keys():
        logger.error("Please specify either volume_id or volume_name")
        return None  # FIXME

    elif "volume_name" in kwargs.keys():
        volume_name = kwargs["volume_name"]

    else:
        # "volume_id" is in kwargs.keys():
        # Used only for logging purposes of the path specified
        volume_name = get_volume_name_from_volume_id(kwargs["volume_id"])

    # Check path is in kwargs
    if "path" not in kwargs.keys():
        # Set to root otherwise
        path = "/"

    else:
        path = kwargs["path"]

    # Check include vars
    if "include" not in kwargs.keys():
        # Add presigned Url and totalItemCount as include values
        kwargs["include"] = "presignedUrl,totalItemCount"

    # Now pull files list from api
    with libgds.ApiClient(ica.configuration(libgds)) as api_client:

        # Initialise all items
        items = []

        # Initialise the api instance
        api_instance = libgds.FilesApi(api_client)

        page_token = None

        while True:
            # Quit only after nextPageToken is cleared
            try:
                api_response: libgds.FileListResponse = api_instance.list_files(**kwargs, page_token=page_token)

            except ApiException as e:
                logger.error("Could not get file id from gds://{}{}".format(volume_name, path))
                return None  # FIXME

            # Append the items
            items.append(api_response.items)

            # Check if there's more items to come
            if getattr(api_response, "nextPageToken", None) is not None:
                # We need to continue iterating until all items are found
                break

            # Set the pageToken from the nextPageToken of the previous output
            page_token = getattr(api_response, "nextPageToken")

    # Return the list of items
    return items


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

    gds_file_list = get_gds_file_list(volume_name=gds_volume_name,
                                      path=gds_path,
                                      include="presignedUrl")

    # Check length is just 1:
    if not len(gds_file_list) == 1:
        logger.error("Please specify a single file. Got {} at gds:{}{}".format(
            len(gds_file_list),
            gds_volume_name,
            gds_path
        ))
        return None  # FIXME

    # Get presigned url
    presigned_url = getattr(gds_file_list[0], "presigned_url", None)

    # Download presigned url through requests library
    gds_req = requests.get(presigned_url)

    # Create an temp file
    content_file = NamedTemporaryFile()

    with open(content_file.name, 'wb') as f:
        f.write(gds_req.content)

    return content_file

