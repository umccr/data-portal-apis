# -*- coding: utf-8 -*-
import json
import logging
from typing import List, Dict

from libumccr.aws import libssm, libsqs
from libica.openapi import libgds
from libica.app import configuration
#from data_processors.lims.lambdas.pierianutils import ica_gds, globals

from libica.openapi.libgds import FileResponse
from urllib.parse import urlparse

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_UMCCRISE_QUEUE_ARN
from data_processors.pipeline.services import metadata_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    logging.warning("CALLED PERFORM")
    output_json = this_workflow.output
    lookup_keys = ['multiqc_output_directory', 'location']
    multiqc_output_directory = liborca.parse_workflow_output(output_json, lookup_keys)

    multiqc_files = collect_gds_multiqc_json_files(multiqc_output_directory['location'])

    for multiqc_file in multiqc_files:
        presignurl = get_presign_url_for_single_file(multiqc_file)
        # call lambda - Q is how
        # print(presignurl)

    #TODO how do we call lambdas from here? 
    #arn:aws:sqs:ap-southeast-2:843407916570:DracarysCdkStack-TriggerDracarysQueueBDD94255-v9akoKRrIeI6

    return {
        "None": None
    }

def get_presign_url_for_single_file(multiqc_file):
    path = multiqc_file.path
    volume_name = multiqc_file.volume_name

    with libgds.ApiClient(configuration(libgds)) as api_client:
        files_api = libgds.FilesApi(api_client)
        try:
            file_list: libgds.FileListResponse = files_api.list_files(
                volume_name=[volume_name],
                path=[path],
                page_size=1000,
                include="presignedUrl"
            )

            if len(file_list.items) != 1:
                logger.error(f"Expected exactly one file at gds://" + volume_name + "/" + path)
                raise FileNotFoundError

            presignurl = file_list.items[0].presigned_url

            # while end
        except libgds.ApiException as e:
            logger.error(f"Exception when calling list_files: \n{e}")    
    return presignurl



def collect_gds_multiqc_json_files(location: str):
    multiqc_json_files = []
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
                print(file_list)
                for item in file_list.items:
                    file: libgds.FileResponse = item

                    if file.name.endswith('multiqc_data.json'):
                        multiqc_json_files.append(file)

                page_token = file_list.next_page_token
                if not file_list.next_page_token:
                    break
            # while end

        except libgds.ApiException as e:
            logger.error(f"Exception when calling list_files: \n{e}")
    return multiqc_json_files

def parse_gds_path(gds_path):
    path_elements = gds_path.replace("gds://", "").split("/")
    volume_name = path_elements[0]
    path_ = f"/{'/'.join(path_elements[1:])}"
    return volume_name, path_
