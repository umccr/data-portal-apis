# -*- coding: utf-8 -*-
"""liborca module

Pronounce "lib·aw·kuh" module. But you understand as Orchestrator related utilities or werkzeug! :D

A catchall module for pipeline Orchestration related functions impl that does not fit elsewhere, yet.

Oh yah, impls are like "killer whale" yosh!! 💪

NOTE: Please retain function into their stateless as much as possible. i.e. in > Fn() > out
Input and output arguments are typically their Primitive forms such as str, int, list, dict, etc..
"""
import logging
from datetime import datetime

from utils import libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_bcl_convert_output(output_json: str) -> list:
    """
    NOTE: as of BCL Convert CWL workflow version 3.7.5, it uses fastq_list_rows format
    Given bcl convert workflow output json, return fastq_list_rows
    See Example IAP Run > Outputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.84abc203cabd4dc196a6cf9bb49d5f74/3.7.5.md

    :param output_json: workflow run output in json format
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """
    output: dict = libjson.loads(output_json)

    lookup_keys = ['main/fastq_list_rows', 'fastq_list_rows']  # lookup in order, return on first found
    look_up_key = None
    for k in lookup_keys:
        if k in output.keys():
            look_up_key = k
            break

    if look_up_key is None:
        raise KeyError(f"Unexpected BCL Convert CWL output format. Expecting one of {lookup_keys}. Found {output.keys()}")

    return output[look_up_key]


def cwl_file_path_as_string_to_dict(file_path):
    """
    Convert "gds://path/to/file" to {"class": "File", "location": "gds://path/to/file"}
    :param file_path:
    :return:
    """

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