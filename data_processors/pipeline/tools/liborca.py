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
import xml.etree.ElementTree as et
from contextlib import closing
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List, Dict, Any

from libica.app import gds
from libumccr import libjson, libregex
from sample_sheet import SampleSheet

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_workflow_output(output_json: str, lookup_keys: List[str]) -> Any:
    """
    Parse workflow run output and return the element for lookup key

    :param lookup_keys: List of string to look up a key from workflow output
    :param output_json: workflow run output in json format
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """

    if not lookup_keys:
        raise ValueError(f"Workflow output lookup_keys is empty: {lookup_keys}")

    output: dict = libjson.loads(output_json)

    look_up_key = None
    for k in lookup_keys:
        if k in output.keys():
            look_up_key = k
            break

    if look_up_key is None:
        raise KeyError(f"Unexpected workflow output format. Expecting one of {lookup_keys}. Found {output.keys()}")

    return output[look_up_key]


def parse_bcl_convert_output(output_json: str, deep_check: bool = True) -> list:
    """
    Parse BCL Convert workflow run output and get fastq_list_rows

    :param output_json: workflow run output in json format
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """

    # output section of interest, lookup in order, return on first found
    lookup_keys = ['main/fastq_list_rows', 'fastq_list_rows']

    fqlr_output = parse_workflow_output(output_json, lookup_keys)

    if deep_check and fqlr_output is None:
        raise ValueError(f"Unexpected bcl_convert output. The fastq_list_rows is {fqlr_output}")

    return fqlr_output


def parse_bcl_convert_output_split_sheets(output_json: str, deep_check: bool = True) -> list:
    """
    Parse BCL Convert workflow run output and get split_sheets

    :param output_json: workflow run output in json format
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return split_sheets: list of split_sheets
    """

    lookup_keys = ['main/split_sheets', 'split_sheets']  # lookup in order, return on first found

    split_sheets = parse_workflow_output(output_json, lookup_keys)

    if deep_check and split_sheets is None:
        raise ValueError(f"Unexpected bcl_convert output. The split_sheets is {split_sheets}")

    return split_sheets


def parse_somatic_workflow_output_directory(output_json: str, deep_check: bool = True) -> Dict:
    """
    Parse the somatic workflow run output and get the output directory of the somatic workflow
    :param output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ['main/dragen_somatic_output_directory', 'dragen_somatic_output_directory']

    dragen_somatic_output_directory = parse_workflow_output(output_json, lookup_keys)

    if deep_check and dragen_somatic_output_directory is None:
        raise ValueError("Could not find a dragen somatic output directory from the somatic+germline workflow")

    return dragen_somatic_output_directory


def parse_germline_workflow_output_directory(output_json: str, deep_check: bool = True) -> Dict:
    """
    Parse the germline workflow run output and get the output directory of the germline workflow
    :param output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ['main/dragen_germline_output_directory', 'dragen_germline_output_directory']

    dragen_germline_output_directory = parse_workflow_output(output_json, lookup_keys)

    if deep_check and dragen_germline_output_directory is None:
        raise ValueError("Could not find a dragen germline output directory from the somatic+germline workflow")

    return dragen_germline_output_directory


def parse_transcriptome_workflow_output_directory(output_json: str, deep_check: bool = True) -> Dict:
    """
    Parse the transcriptome workflow run output and get the output directory of the transcriptome workflow
    :param output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ['main/dragen_transcriptome_output_directory', 'dragen_transcriptome_output_directory']

    dragen_transcriptome_output_directory = parse_workflow_output(output_json, lookup_keys)

    if deep_check and dragen_transcriptome_output_directory is None:
        raise ValueError("Could not find a dragen transcriptome output directory from the transcriptome workflow")

    return dragen_transcriptome_output_directory


def parse_arriba_workflow_output_directory(output_json: str, deep_check: bool = True) -> Dict:
    """
    Parse the arriba workflow run output and get the output directory of the arriba workflow
    :param output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ['main/arriba_output_directory', 'arriba_output_directory']

    arriba_output_directory = parse_workflow_output(output_json, lookup_keys)

    if deep_check and arriba_output_directory is None:
        raise ValueError("Could not find a dragen arriba output directory from the arriba workflow")

    return arriba_output_directory


def parse_umccrise_workflow_output_directory(output_json: str, deep_check: bool = True) -> Dict:
    """
    Parse the umccrise workflow run output and get the output directory of the umccrise workflow
    :param output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = [
        # Old keys < 2023.01
        'main/umccrise_output_directory', 'umccrise_output_directory',
        # New keys > 2023.01
        'main/output_directory', 'output_directory'
    ]

    umccrise_output_directory = parse_workflow_output(output_json, lookup_keys)

    if deep_check and umccrise_output_directory is None:
        raise ValueError("Could not find a umccrise output directory from the umccrise workflow")

    return umccrise_output_directory


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


def strip_topup_rerun_from_library_id_list(library_id_list: List[str]) -> List[str]:
    rglb_id_set = set()
    for library_id in library_id_list:

        # Strip _topup
        rglb = libregex.SAMPLE_REGEX_OBJS['topup'].split(library_id, 1)[0]

        # Strip _rerun
        rglb = libregex.SAMPLE_REGEX_OBJS['rerun'].split(rglb, 1)[0]

        rglb_id_set.add(rglb)

    return list(rglb_id_set)


def strip_topup_rerun_from_library_id(library_id: str) -> str:
    return strip_topup_rerun_from_library_id_list([library_id])[0]


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


def get_runinfo(gds_volume: str, runinfo_path: str) -> et.Element:
    # TODO: represent RunInfo better, perhaps as domain object?
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
    return tree.getroot()


def get_run_config_from_runinfo(gds_volume: str, runinfo_path: str) -> str:
    root = get_runinfo(gds_volume=gds_volume, runinfo_path=runinfo_path)
    cyc = {}
    for read in root.findall('Run/Reads/Read'):
        cyc[read.get('Number')] = read.get('NumCycles')

    run_config = {
        "RunCycles": f"{cyc['1']},{cyc['2']},{cyc['3']},{cyc['4']}"
    }

    logger.info(f"Extracted run config: {run_config}")

    return json.dumps(run_config)


def get_number_of_lanes_from_runinfo(gds_volume, runinfo_path) -> int:
    root = get_runinfo(gds_volume=gds_volume, runinfo_path=runinfo_path)

    fcl = root.find('Run/FlowcellLayout')
    lane_cnt = fcl.get('LaneCount')
    return int(lane_cnt)


def parse_wgs_alignment_qc_output_for_bam_file(workflow_output_json: str, deep_check: bool = True) -> str:
    """
    Parse the bam file out of the DRAGEN_WGS_QC (wgs_alignment_qc) workflow

    :param workflow_output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ["dragen_bam_out"]

    dragen_bam_out: Dict = parse_workflow_output(workflow_output_json, lookup_keys)

    if deep_check:
        if dragen_bam_out is None:
            raise ValueError(f"Unexpected wgs_alignment_qc output. The dragen_bam_out is {dragen_bam_out}")

        if 'location' not in dragen_bam_out.keys():
            raise ValueError(f"Unexpected wgs_alignment_qc output. The dragen_bam_out has no location to BAM file")

    return dragen_bam_out['location']


def parse_wts_alignment_qc_output_for_bam_file(workflow_output_json: str, deep_check: bool = True) -> str:
    """
    Parse the bam file out of the DRAGEN_WTS_QC (wts_alignment_qc) workflow

    :param workflow_output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ["dragen_bam_out"]

    dragen_bam_out: Dict = parse_workflow_output(workflow_output_json, lookup_keys)

    if deep_check:
        if dragen_bam_out is None:
            raise ValueError(f"Unexpected wts_alignment_qc output. The dragen_bam_out is {dragen_bam_out}")

        if 'location' not in dragen_bam_out.keys():
            raise ValueError(f"Unexpected wts_alignment_qc output. The dragen_bam_out has no location to BAM file")

    return dragen_bam_out['location']


def parse_transcriptome_output_for_bam_file(workflow_output_json: str, deep_check: bool = True) -> str:
    """
    Parse the bam file out of the transcriptome (DRAGEN_WTS or wts_tumor_only) workflow

    WTS Workflow output
    dragen_transcriptome_output_directory -> *.bam

    :param workflow_output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    transcriptome_output_directory: Dict = parse_transcriptome_workflow_output_directory(workflow_output_json)

    if deep_check and 'location' not in transcriptome_output_directory.keys():
        raise ValueError(f"Unexpected wts_tumor_only output. "
                         f"No location key found for dragen_transcriptome_output_directory")

    transcriptome_output_location = transcriptome_output_directory['location']

    bam_files = gds.get_files_from_gds_by_suffix(
        location=transcriptome_output_location,
        file_suffix=".bam"
    )

    # assuming it should have only 1 BAM file in output directory
    if not len(bam_files) == 1:
        raise ValueError(f"Couldn't get bam file from transcriptome output directory '{transcriptome_output_location}'")

    return bam_files[0]


def parse_tso_ctdna_output_for_bam_file(workflow_output_json: str, deep_check: bool = True) -> str:
    """
    Parse the bam file out of the TSO ctDNA (DRAGEN_TSO_CTDNA or tso_ctdna_tumor_only) workflow

    :param workflow_output_json:
    :param deep_check: default to True to raise ValueError if the output section of interest is None
    :return:
    """

    lookup_keys = ["output_results_dir_by_sample"]

    output_results_dir_by_sample: List = parse_workflow_output(workflow_output_json, lookup_keys)

    if deep_check:
        if output_results_dir_by_sample is None:
            raise ValueError(f"Unexpected tso_ctdna_tumor_only output. The output_results_dir_by_sample is None")

        if not len(output_results_dir_by_sample) == 1:
            raise ValueError(f"Unexpected tso_ctdna_tumor_only output. Can't handle multiple samples output")

    results_by_sample = output_results_dir_by_sample[0]

    listing: List = results_by_sample['listing']  # get listing of this directory

    for listed_item in listing:
        # Only interested in bam files
        if not listed_item.get("nameext", None) == ".bam":
            continue

        # Collect the bam that isn't cleaned.stitched.bam OR the evidence bam
        if listed_item.get("basename", "").startswith("evidence."):
            continue
        if listed_item.get("basename", "").endswith("cleaned.stitched.bam"):
            continue

        # Found
        main_bam_path = listed_item.get("location")

        # Make a check that it really exists in GDS
        matched_bam_files = gds.check_file(gds_path=main_bam_path)

        if not len(matched_bam_files) == 1:
            raise ValueError(f"Couldn't get bam file from tso_ctdna_tumor_only output directory '{main_bam_path}'")

        return main_bam_path
