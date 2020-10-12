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

from libiap.openapi import libgds

from utils import libssm, libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_IAP_BASE_URL = "https://aps2.platform.illumina.com"


def configuration():
    iap_auth_token = os.getenv("IAP_AUTH_TOKEN", None)
    if iap_auth_token is None:
        iap_auth_token = libssm.get_secret('/iap/jwt-token')
    iap_base_url = os.getenv("IAP_BASE_URL", DEFAULT_IAP_BASE_URL)

    config = libgds.Configuration(
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


def handler(event, context) -> dict:
    """event payload dict
    {
        'gds_path': "gds://volume/path/to/fastq"
    }

    Given a path to FASTQ files, this handler produce the following structure.

    The fastq_map is backed by nested dict and it has sample_name as index and corresponding fastq_list.
    It is flexible, in a sense, the inner dict can get expanded as need be, i.e. act more like 'bag' struct to collect
    matching Sample's FASTQ files with easy recall sample name index. Optionally tags for grouping purpose.
    {
        'volume_name': "volume name",
        'path': "folder path",
        'gds_path': "gds://volume/path/to/fastq",
        'fastq_map': {
            'SAMPLE_NAME1': {
                'fastq_list': ['gds://vol/abs/path/SAMPLE_NAME1_S1_L001_R1_001.fastq.gz', 'gds://vol/abs/path/SAMPLE_NAME1_S1_L001_R2_001.fastq.gz', ...],
                'tags': ['optional', 'aggregation', 'tag', 'for_example', 'SBJ00001', ...],
                ...
            },
            'SAMPLE_NAME2': {
                'fastq_list': ['gds://vol/abs/path/SAMPLE_NAME2_S1_L001_R1_001.fastq.gz', 'gds://vol/abs/path/SAMPLE_NAME2_S1_L001_R2_001.fastq.gz', ...],
                'tags': ['optional', 'aggregation', 'tag', 'for_example', 'SBJ00001', ...],
                ...
            },
            ...
        }
    }

    The return fastq_container is memory bound. However, building few samples is efficient. If one wish to build
    hundreds and hundreds K of samples, it can simply achieve using Python generator/yield to stream the result.

    TODO tag SBJ ID from Portal LIMS database
        option#1 search LIMSRow by sample name as each sample enumeration -- IO bound, hitting db per sample
        option#2 read all LIMSRow once into memory, then filter within Python construct -- Memory bound

    TODO consider refactor so that it will also works for AWS S3 and/or file system

    TODO consider streaming the result version as alternate impl

    TODO consider generalise reusable library module/package e.g. 'libfastq' so that 'pip install libfastq'

    :param event:
    :param context:
    :return: fastq container as nested dict
    """

    logger.info(f"Start processing FASTQ event")
    logger.info(libjson.dumps(event))

    gds_path: str = event['gds_path']
    volume_name, path_ = parse_gds_path(gds_path)

    fastq_container = {}
    fastq_container.update(volume_name=volume_name)
    fastq_container.update(path=path_)
    fastq_container.update(gds_path=gds_path)

    fastq_map = defaultdict(dict)

    with libgds.ApiClient(configuration()) as api_client:
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
                    if not file.name.endswith('.fastq.gz'):
                        continue
                    sample_name = extract_fastq_sample_name(file.name)
                    if sample_name:
                        file_abs_path = f"gds://{file.volume_name}{file.path}"  # transform to absolute gds path
                        if sample_name in fastq_map:
                            fastq_map[sample_name]['fastq_list'].append(file_abs_path)  # append more if the same sample
                        else:
                            fastq_map[sample_name]['fastq_list'] = [file_abs_path]  # first one
                            fastq_map[sample_name]['tags'] = []  # TODO tag SBJ ID from Portal LIMS database
                    else:
                        logger.info(f"Failed to extract sample name from file: {file.name}")

                page_token = file_list.next_page_token
                if not file_list.next_page_token:
                    break
            # while end

            fastq_container.update(fastq_map=fastq_map)

        except libgds.ApiException as e:
            logger.error(f"Exception when calling list_files: \n{e}")

    logger.info(libjson.dumps(fastq_container))

    return fastq_container
