try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
from data_processors.pipeline.services import gds_srv
from libica.app import configuration
from libica.openapi import libgds
from libumccr import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_presigned_url(file_id: str):
    logger.debug(f"Retrieving pre-signed URL for file: {file_id}")
    with libgds.ApiClient(configuration(libgds)) as api_client:
        file_api = libgds.FilesApi(api_client)
    try:
        file_details: libgds.FileListResponse = file_api.get_file(file_id=file_id)
    except libgds.ApiException as e:
        logger.info("Exception when calling FilesApi: %s\n" % e)

    logger.info("FILE DETAILS")
    logger.info(file_details)

    return file_details.presigned_url


def handler(event, context):
    """
    Event payload structure:
    {
      "gds_volume_name": "umccr-fastq-data",
      "tokens': ["Foo", "Bar", "fastq.gz"],
      "regex": ".+FooBar.+fastq.gz",
      "presigned": "True"
    }
    Where
    - "gds_volume_name" is the name of the GDS volume containing the files
    - "tokens" is a list of keywords the GDS path has to contain
    - "regex" is a regex pattern the GDS file path has to match
    - "tokens" and "regex" are mutually exclusive
    - "presigned" adds S3 pre-signed URLs for the GDS file, default = False

    :param event:
    :param context:
    :return:
    """
    logger.info(f"Start GDS file search lambda")
    logger.info(libjson.dumps(event))

    gds_volume_name = event['gds_volume_name']
    presigned = event.get('presigned')
    if presigned not in ["TRUE", "True", "true", True]:
        presigned = False

    if not ('regex' in event) ^ ('tokens' in event):
        logger.error("Mutual exclusive options! Provide either 'regex' or 'tokens'.")
        exit(1)

    regex = event.get('regex')
    if regex:
        logger.info("Proceeding with regex")
        query_set = gds_srv.get_gds_files_for_regex(volume_name=gds_volume_name, pattern=regex)

    tokens = event.get('tokens')
    if tokens:
        logger.info("Proceeding with tokens")
        query_set = gds_srv.get_gds_files_for_path_tokens(volume_name=gds_volume_name, path_tokens=tokens)

    if not query_set.exists():
        logger.error(f"No GDS file records found for search criteria: {libjson.dumps(event)}")

    results = {'volume': gds_volume_name, 'files': []}
    for record in query_set:
        results['files'].append({'path': record.path, 'id': record.file_id})

    if presigned:
        for file in results['files']:
            file['presigned_url'] = get_presigned_url(file_id=file['id'])

    return results
