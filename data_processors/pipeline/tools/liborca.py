# -*- coding: utf-8 -*-
"""liborca module

Pronounce "lib·aw·kuh" module. But you understand as Orchestrator related utilities or werkzeug! :D

A catchall module for pipeline Orchestration related functions impl that does not fit elsewhere, yet.

Oh yah, impls are like "killer whale" yosh!! 💪

NOTE: Please retain function into their _stateless_ as much as possible. i.e. in > Fn() > out
Input and output arguments are typically their _Primitive_ forms such as str, int, list, dict, etc..
"""
import json
import logging
import os
import re
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


def strip_topup_rerun_from_library_id(library_id: str) -> str:
    """
    Use some fancy regex to remove _topup or _rerun from library id
    ... well for now just some regex, pls don't try to understand it
    https://regex101.com/r/Z8IG4T/1
    :return:
    """
    # TODO refactor this into libregex
    library_id_regex = re.match(r"(L\d{7}|L(?:(?:PRJ|CCR|MDX|TGX)\d{6}|(?:NTC|PTC)_\w+))(?:_topup\d?|_rerun\d?)?", library_id)

    if library_id_regex is None:
        logger.warning(f"Could not get library id from {library_id}, returning as is")
        return library_id

    return library_id_regex.group(1)


def sample_library_id_has_rerun(sample_library_id: str) -> bool:
    """
    Check if a library id has a rerun suffix

    :param sample_library_id: as in SampleSheet v1 {sample_id}_{library_id}
    :return:
    """
    # TODO refactor this into libregex
    library_id_rerun_regex = re.match(r"(?:(?:PRJ|CCR|MDX|TGX)\d{6}|(?:NTC|PTC)_\w+)_(?:L\d{7}|L(?:(?:PRJ|CCR|MDX|TGX)\d{6}|(?:NTC|PTC)_\w+))(?:_topup\d?)?(_rerun\d?)?", sample_library_id)

    if library_id_rerun_regex is None:
        return False

    if library_id_rerun_regex.group(1) is not None:
        return True

    return False


def get_library_id_from_sample_name(sample_name: str):
    # format: samplename_libraryid_extension
    # we are only interested in the library ID
    fragments = sample_name.split("_")
    # if there is an extension, the library ID is the second to last fragment
    if "_topup" in sample_name or "_rerun" in sample_name:
        return fragments[-2]
    # if not, then the library ID is the last fragment
    return fragments[-1]


def get_samplesheet(gds_volume: str, samplesheet_path: str) -> SampleSheet:
    if not samplesheet_path.startswith(os.path.sep):
        samplesheet_path = os.path.sep + samplesheet_path

    ntf: NamedTemporaryFile = gds.download_gds_file(gds_volume, samplesheet_path)
    if ntf is None:
        reason = f"Can not download sample sheet from GDS: gds://{gds_volume}{samplesheet_path}"
        logger.error(reason)
        raise ValueError(reason)

    logger.debug(f"Local sample sheet path: {ntf.name}")

    with closing(ntf) as f:
        return SampleSheet(f.name)


def get_samplesheet_to_json(gds_volume: str, samplesheet_path: str) -> str:
    return get_samplesheet(gds_volume, samplesheet_path).to_json()


def get_sample_names_from_samplesheet(gds_volume: str, samplesheet_path: str) -> List[str]:
    logger.info(f"Extracting sample names from gds://{gds_volume}{samplesheet_path}")
    sample_names = set()
    samplesheet = get_samplesheet(gds_volume, samplesheet_path)
    for sample in samplesheet:
        sample_names.add(sample.Sample_ID)

    logger.info(f"Extracted sample names: {sample_names}")

    return list(sample_names)


# TODO: combine with above?
def get_samplesheet_json_from_file(gds_volume: str, samplesheet_path: str) -> str:
    # TODO: represent SampleSheet better, perhaps as domain object?
    if not samplesheet_path.startswith(os.path.sep):
        samplesheet_path = os.path.sep + samplesheet_path
    logger.info(f"Extracting samplesheet config from gds://{gds_volume}{samplesheet_path}")

    ntf: NamedTemporaryFile = gds.download_gds_file(gds_volume, samplesheet_path)
    if ntf is None:
        reason = f"Abort extracting metadata process. " \
                 f"Cannot download file from GDS: gds://{gds_volume}{samplesheet_path}"
        logger.error(reason)
        raise ValueError(reason)

    logger.info(f"Local sample sheet path: {ntf.name}")
    with closing(ntf) as f:
        samplesheet = SampleSheet(f.name)
        samplesheet_config = samplesheet.to_json()

    logger.info(f"Extracted samplesheet config: {samplesheet_config}")

    return samplesheet_config


def get_run_config_from_runinfo(gds_volume: str, runinfo_path: str) -> str:
    # TODO: represent RunInfo better, perhaps as domain object?
    import xml.etree.ElementTree as et
    if not runinfo_path.startswith(os.path.sep):
        runinfo_path = os.path.sep + runinfo_path
    logger.info(f"Extracting run config from gds://{gds_volume}{runinfo_path}")

    ntf: NamedTemporaryFile = gds.download_gds_file(gds_volume, runinfo_path)
    if ntf is None:
        reason = f"Abort extracting metadata process. " \
                 f"Can not download file from GDS: gds://{gds_volume}{runinfo_path}"
        logger.error(reason)
        raise ValueError(reason)

    logger.info(f"Local sample sheet path: {ntf.name}")
    tree = et.ElementTree(file=ntf)
    root = tree.getroot()
    cyc = {}
    for read in root.findall('Run/Reads/Read'):
        cyc[read.get('Number')] = read.get('NumCycles')

    run_config = {
        "RunCycles": f"{cyc['1']},{cyc['2']},{cyc['3']},{cyc['4']}"
    }

    logger.info(f"Extracted run config: {run_config}")

    return json.dumps(run_config)
