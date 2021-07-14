# -*- coding: utf-8 -*-
"""dragen_tso_ctdna_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
import re
from pathlib import Path
from typing import List

import pandas as pd

from data_portal.models import Workflow, LabMetadata, LabMetadataPhenotype, LabMetadataWorkflow, LabMetadataType, \
    LabMetadataAssay
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.config import SQS_DRAGEN_TSO_CTDNA_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import batch_srv, fastq_srv
from data_processors.pipeline.tools import liborca
from utils import libssm, libsqs, libjson

# GLOBALS
SAMPLESHEET_ASSAY_TYPE_REGEX = r"^(?:SampleSheet\.)(\S+)(?:\.csv)$"
CTTSO_ASSAY_TYPE = ["ctDNA_ctTSO", "ctTSO_ctTSO"]  # FIXME this will conform to one of these elements at some point.

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):

    batcher = Batcher(
        workflow=this_workflow,
        run_step=WorkflowType.DRAGEN_TSO_CTDNA.value.upper(),
        batch_srv=batch_srv,
        fastq_srv=fastq_srv,
        logger=logger,
    )

    if batcher.batch_run is None:
        return batcher.get_skip_message()

    # prepare job list and dispatch to job queue
    job_list = prepare_dragen_tso_ctdna_jobs(batcher)
    if job_list:
        libsqs.dispatch_jobs(
            queue_arn=libssm.get_ssm_param(SQS_DRAGEN_TSO_CTDNA_QUEUE_ARN),
            job_list=job_list
        )
    else:
        batcher.reset_batch_run()  # reset running if job_list is empty

    return batcher.get_status()


def prepare_dragen_tso_ctdna_jobs(batcher: Batcher) -> List[dict]:
    """
    NOTE: This launches the cttso cwl workflow that mimics the ISL workflow
    See Example IAP Run > Inputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.5cc28c147e4e4dfa9e418523188aacec/3.7.5--1.3.5.md

    ctTSO job preparation is at _pure_ Library level aggregate.
    Here "Pure" Library ID means without having _topup(N) or _rerun(N) suffixes.
    The fastq_list_row lambda already stripped these topup/rerun suffixes (i.e. what is in this_batch.context_data cache).
    Therefore, it aggregates all fastq list at
        - per sequence run by per library for
            - all different lane(s)
            - all topup(s)
            - all rerun(s)
    This constitute one ctTSO job (i.e. one ctTSO workflow run).

    See test_prepare_dragen_tso_ctdna_jobs() for example job list of validation run.

    :param batcher:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(batcher.batch.context_data)

    # iterate through each sample group by rglb
    for rglb, rglb_df in pd.DataFrame(fastq_list_rows).groupby("rglb"):
        # Check rgsm is identical
        # .item() will raise error if there exists more than one sample name for a given library
        rgsm = rglb_df['rgsm'].unique().item()

        # Get the metadata for the library
        # NOTE: this will use the library base ID (i.e. without topup/rerun extension), as the metadata is the same
        lib_metadata: LabMetadata = LabMetadata.objects.get(library_id=rglb)
        # make sure we have recognised sample (e.g. not undetermined)
        if not lib_metadata:
            logger.error(f"SKIP DRAGEN_TSO_CTDNA workflow for {rgsm}_{rglb}. "
                         f"No metadata for {rglb}, this should not happen!")
            continue

        # skip negative control samples
        if lib_metadata.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower():
            logger.info(f"SKIP DRAGEN_TSO_CTDNA workflow for '{rgsm}_{rglb}'. Negative-control.")
            continue

        # Skip samples where metadata workflow is set to manual
        if lib_metadata.workflow.lower() == LabMetadataWorkflow.MANUAL.value.lower():
            # We do not pursue manual samples
            logger.info(f"SKIP DRAGEN_TSO_CTDNA workflow for '{rgsm}_{rglb}'. Workflow set to manual.")
            continue

        # skip if assay is not CT_TSO and type is not CT_DNA
        if not (lib_metadata.type.lower() == LabMetadataType.CT_DNA.value.lower() and
                lib_metadata.assay.lower() == LabMetadataAssay.CT_TSO.value.lower()):
            logger.warning(f"SKIP DRAGEN_TSO_CTDNA workflow for '{rgsm}_{rglb}'. "
                           f"Type: 'ctDNA' != '{lib_metadata.type}' or Assay: 'ctTSO' != '{lib_metadata.assay}'")
            continue

        # Sample ID
        samplesheet_sample_id = rglb_df['rgid'].apply(lambda x: x.rsplit(".", 1)[-1]).unique().item()

        # Get samplesheet and run files from bcl run
        samplesheet = get_ct_tso_samplesheet_from_bcl_convert_output(batcher.workflow.output)
        run_info_xml, run_parameters_xml = get_run_xml_files_from_bcl_convert_workflow(batcher.workflow.input)

        # Update read 1 and read 2 strings to cwl file paths
        rglb_df["read_1"] = rglb_df["read_1"].apply(liborca.cwl_file_path_as_string_to_dict)
        rglb_df["read_2"] = rglb_df["read_2"].apply(liborca.cwl_file_path_as_string_to_dict)

        job = {
            "tso500_sample": {
                "sample_id": f"{samplesheet_sample_id}",  # This must match the sample sheet
                "sample_name": f"{rgsm}",
                "sample_type": "DNA",
                "pair_id": f"{rgsm}"
            },
            "fastq_list_rows": rglb_df.to_dict(orient="records"),
            "samplesheet": liborca.cwl_file_path_as_string_to_dict(samplesheet),
            "run_info_xml": liborca.cwl_file_path_as_string_to_dict(run_info_xml),
            "run_parameters_xml": liborca.cwl_file_path_as_string_to_dict(run_parameters_xml),
            "seq_run_id": batcher.sqr.run_id if batcher.sqr else None,
            "seq_name": batcher.sqr.name if batcher.sqr else None,
            "batch_run_id": int(batcher.batch_run.id)
        }

        job_list.append(job)

    return job_list


def get_ct_tso_samplesheet_from_bcl_convert_output(workflow_output):
    """
    Get the gds path containing the samplesheet used for splitting ctTSO samples
    """

    split_sheets = liborca.parse_bcl_convert_output_split_sheets(workflow_output)

    samplesheet_locations = []
    for samplesheet in split_sheets:
        samplesheet_locations.append(samplesheet['location'])

    for samplesheet_location in samplesheet_locations:
        samplesheet_path_obj = Path(samplesheet_location)  # Note Path object is not URI scheme safe, it loose a slash
        samplesheet_name = samplesheet_path_obj.name
        regex_obj = re.fullmatch(SAMPLESHEET_ASSAY_TYPE_REGEX, samplesheet_name)
        if regex_obj is None:
            logger.warning(f"Could not get SampleSheet '{samplesheet_name}' to fit"
                           f" into regex '{SAMPLESHEET_ASSAY_TYPE_REGEX}'")
            continue
        # Collect midfix of samplesheet
        assay_type = regex_obj.group(1)

        if assay_type in CTTSO_ASSAY_TYPE:
            return str(samplesheet_location)

    logger.warning("Did not get a ct tso samplesheet from the bclconvert output")
    return None


def get_run_xml_files_from_bcl_convert_workflow(bcl_convert_input):
    """
    From the input object, get the bcl_input_directory directory value and add 'RunInfo.xml' and 'RunParameters.xml'
    :param bcl_convert_input:
    :return:
    """

    bcl_convert_input = json.loads(bcl_convert_input)

    bcl_input_dir = bcl_convert_input['bcl_input_directory']['location']

    return f"{bcl_input_dir}/RunInfo.xml", f"{bcl_input_dir}/RunParameters.xml"
