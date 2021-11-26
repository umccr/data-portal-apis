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
import re
from collections import defaultdict
from typing import List

from libica.app import configuration
from libica.openapi import libgds

from data_processors.pipeline.services import metadata_srv
from libumccr import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """event payload dict
    {
        'locations': [
            "gds://volume/run1/path/to/fastq",
            "gds://volume/run2/path/to/fastq",
        ],
        'project_owner': [
            "UMCCR",
        ],
        'flat': False,
    }

    Given locations, find FASTQ files in there recursively if needed.

    :param event:
    :param context:
    :return: fastq container
    """

    logger.info(f"Start processing FASTQ event")
    logger.info(libjson.dumps(event))

    locations: List[str] = event['locations']
    project_owner_list: List[str] = event.get('project_owner', None)
    flatten: bool = event.get('flat', False)

    fastq_flatten_list = []
    fastq_list_csv_files = []
    fastq_files = []
    for location in locations:
        if location.endswith('/'):
            location = location[:-1]
        collect_gds_files(location, fastq_list_csv_files, fastq_files)

    fastq_map = defaultdict(dict)
    for file in fastq_files:
        sample_name = extract_fastq_sample_name(file.name)

        if sample_name:
            meta = metadata_srv.get_metadata_by_sample_library_name_as_in_samplesheet(sample_library_name=sample_name)
            tags = []
            if meta:

                # filter out project_owner of interest
                if project_owner_list and meta.project_owner not in project_owner_list:
                    continue

                tags = [
                    {
                        'subject_id': meta.subject_id,
                        'project_owner': meta.project_owner,
                        'project_name': meta.project_name,
                    },
                ]

            file_abs_path = f"gds://{file.volume_name}{file.path}"
            if flatten:
                fastq_flatten_list.append(file_abs_path)
                continue

            fastq_directory = os.path.dirname(file_abs_path)

            selected_csv_file_abs_path = ""
            for csv_file in fastq_list_csv_files:
                csv_file_abs_path = f"gds://{csv_file.volume_name}{csv_file.path}"
                if csv_file_abs_path.replace("/Reports/fastq_list.csv", "") in fastq_directory:
                    selected_csv_file_abs_path = csv_file_abs_path

            if sample_name in fastq_map:
                fastq_map[sample_name]['fastq_list'].append(file_abs_path)
                if fastq_directory not in fastq_map[sample_name]['fastq_directories']:
                    fastq_map[sample_name]['fastq_directories'].append(fastq_directory)
                if selected_csv_file_abs_path not in fastq_map[sample_name]['fastq_list_csv']:
                    fastq_map[sample_name]['fastq_list_csv'].append(selected_csv_file_abs_path)
            else:
                fastq_map[sample_name]['fastq_list'] = [file_abs_path]
                fastq_map[sample_name]['fastq_directories'] = [fastq_directory]
                fastq_map[sample_name]['fastq_list_csv'] = [selected_csv_file_abs_path]
                fastq_map[sample_name]['tags'] = tags
        else:
            logger.info(f"Failed to extract sample name from file: {file.name}")

    if flatten:
        logger.info(libjson.dumps(fastq_flatten_list))
        return fastq_flatten_list

    fastq_container = {}
    fastq_container.update(locations=event['locations'])
    fastq_container.update(fastq_map=fastq_map)
    logger.info(libjson.dumps(fastq_container))
    return fastq_container


def collect_gds_files(location: str, fastq_list_csv_files: List, fastq_files: List):
    volume_name, path_ = parse_gds_path(location)

    with libgds.ApiClient(configuration(libgds)) as api_client:
        files_api = libgds.FilesApi(api_client)
        try:
            page_token = None
            while True:
                file_list: libgds.FileListResponse = files_api.list_files(
                    volume_name=[volume_name],
                    path=[f"{path_}/*"],
                    page_size=1000,
                    page_token=page_token,
                )

                for item in file_list.items:
                    file: libgds.FileResponse = item

                    if file.name.endswith('fastq_list.csv'):
                        fastq_list_csv_files.append(file)

                    if file.name.endswith('.fastq.gz'):
                        fastq_files.append(file)

                page_token = file_list.next_page_token
                if not file_list.next_page_token:
                    break
            # while end

        except libgds.ApiException as e:
            logger.error(f"Exception when calling list_files: \n{e}")


def extract_fastq_sample_name(filename):
    """
    Extract sample_id from FASTQ file name based on BSSH FASTQ filename Naming Convention.
    https://emea.support.illumina.com/help/BaseSpace_OLH_009008/Content/Source/Informatics/BS/NamingConvention_FASTQ-files-swBS.htm
    :param filename:
    :return: sample_name or None
    """
    sample_name = re.split('_S[0-9]+_', filename)[0].rstrip('_')
    if sample_name == filename:  # i.e. can't split by regex rule
        return None
    return sample_name


def parse_gds_path(gds_path):
    path_elements = gds_path.replace("gds://", "").split("/")
    volume_name = path_elements[0]
    path_ = f"/{'/'.join(path_elements[1:])}"
    return volume_name, path_
