# -*- coding: utf-8 -*-
"""liborca module

Pronounce "lib·aw·kuh" module. But you understand as Orchestrator related utilities or werkzeug! :D

A catchall module for pipeline Orchestration related functions impl that does not fit elsewhere, yet.

Oh yah, impls are like "killer whale" yosh!! 💪

NOTE: Please retain function into their stateless as much as possible. i.e. in > Fn() > out
Input and output arguments are typically their Primitive forms such as str, int, list, dict, etc..
"""
from data_portal.models import LabMetadata
import logging
import os
from contextlib import closing
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List

from sample_sheet import SampleSheet

from utils import libjson, gds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_bcl_convert_output(output_json: str, lookup_keys=None) -> list:
    """
    Parse BCL Convert workflow run output and get fastq_list_rows

    :param lookup_keys: List of string to lookup a key from BCL Convert output
    :param output_json: workflow run output in json format
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """
    output: dict = libjson.loads(output_json)

    default_lookup_keys = ['main/fastq_list_rows', 'fastq_list_rows']  # lookup in order, return on first found

    if lookup_keys is None:
        lookup_keys = default_lookup_keys

    look_up_key = None
    for k in lookup_keys:
        if k in output.keys():
            look_up_key = k
            break

    if look_up_key is None:
        raise KeyError(f"Unexpected BCL Convert output format. Expecting one of {lookup_keys}. Found {output.keys()}")

    return output[look_up_key]


def parse_bcl_convert_output_split_sheets(output_json: str) -> list:
    """
    Parse BCL Convert workflow run output and get split_sheets

    :param output_json: workflow run output in json format
    :return split_sheets: list of split_sheets
    """

    lookup_keys = ['main/split_sheets', 'split_sheets']  # lookup in order, return on first found

    return parse_bcl_convert_output(output_json, lookup_keys)


def cwl_file_path_as_string_to_dict(file_path):
    """
    Convert "gds://path/to/file" to {"class": "File", "location": "gds://path/to/file"}
    :param file_path:
    :return:
    """

    if isinstance(file_path, dict):
        keys = file_path.keys()
        if "class" in keys and "location" in keys:
            if file_path['class'] == "File":
                # it seems file_path is already in CWL File object form, return as-is
                return file_path

    return {"class": "File", "location": file_path}


def cwl_dir_path_as_string_to_dict(dir_path):
    """
    Convert "gds://path/to/dir to {"class": "Directory", "location": "gds://path/to/dir"}
    :param dir_path:
    :return:
    """
    return {"class": "Directory", "location": dir_path}


def get_run_number_from_run_name(run_name: str) -> int:
    return int(run_name.split('_')[2])


def get_timestamp_from_run_name(run_name: str) -> str:
    date_part = run_name.split('_')[0]
    # convert to format YYYY-MM-DD
    return datetime.strptime(date_part, '%y%m%d').strftime('%Y-%m-%d')


def get_library_id_from_sample_name(sample_name: str):
    # format: samplename_libraryid_extension
    # we are only interested in the library ID
    fragments = sample_name.split("_")
    # if there is an extension, the library ID is the second to last fragment
    if "_topup" in sample_name or "_rerun" in sample_name:
        return fragments[-2]
    # if not, then the library ID is the last fragment
    return fragments[-1]


def get_sample_names_from_samplesheet(gds_volume: str, samplesheet_path: str) -> List[str]:
    if not samplesheet_path.startswith(os.path.sep):
        samplesheet_path = os.path.sep + samplesheet_path
    logger.info(f"Extracting sample names from gds://{gds_volume}{samplesheet_path}")

    ntf: NamedTemporaryFile = gds.download_gds_file(gds_volume, samplesheet_path)
    if ntf is None:
        reason = f"Abort extracting metadata process. " \
                 f"Can not download sample sheet from GDS: gds://{gds_volume}{samplesheet_path}"
        logger.error(reason)
        raise ValueError(reason)

    logger.info(f"Local sample sheet path: {ntf.name}")
    sample_names = set()
    with closing(ntf) as f:
        samplesheet = SampleSheet(f.name)
        for sample in samplesheet:
            sample_names.add(sample.Sample_ID)

    logger.info(f"Extracted sample names: {sample_names}")

    return list(sample_names)


def get_subject_id_from_libary_id(library_id):
    """
    Get subject from a library id through metadata objects list
    :param library_id:
    :return:
    """
    try:
        subject_id = LabMetadata.objects.get(library_id=library_id).subject_id
    except LabMetadata.DoesNotExist:
        subject_id = None
        logger.error(f"No subject for library {library_id}")

    return subject_id

