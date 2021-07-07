# -*- coding: utf-8 -*-
"""wgs_qc_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List
import json

import pandas as pd

from data_portal.models import Batch, BatchRun, SequenceRun, LabMetadata, LabMetadataPhenotype, LabMetadataWorkflow, \
    LabMetadataType, LabMetadataAssay
from data_processors.pipeline.domain.config import SQS_DRAGEN_TSO_CTDNA_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.lambdas import fastq_list_row
from data_processors.pipeline.services import batch_srv, fastq_srv
from data_processors.pipeline.tools import liborca
from utils import libssm, libsqs, libjson
from pathlib import Path
import re

# GLOBALS
SAMPLESHEET_ASSAY_TYPE_REGEX = r"^(?:SampleSheet\.)(\S+)(?:\.csv)$"
CTTSO_ASSAY_TYPE = [ "ctDNA_ctTSO", "ctTSO_ctTSO" ]  # FIXME this will conform to one of these elements at some point.

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_sqr, this_workflow):
    # create a batch if not exist
    batch_name = this_sqr.name if this_sqr else f"{this_workflow.type_name}__{this_workflow.wfr_id}"
    this_batch = batch_srv.get_or_create_batch(name=batch_name, created_by=this_workflow.wfr_id)

    # register a new batch run for this_batch run step
    this_batch_run = batch_srv.skip_or_create_batch_run(
        batch=this_batch,
        run_step=WorkflowType.DRAGEN_TSO_CTDNA.value.upper()
    )
    if this_batch_run is None:
        # skip the request if there is on going existing batch_run for the same batch run step
        # this is especially to fence off duplicate IAP WES events hitting multiple time to our IAP event lambda
        msg = f"SKIP. THERE IS EXISTING ON GOING RUN FOR BATCH " \
              f"ID: {this_batch.id}, NAME: {this_batch.name}, CREATED_BY: {this_batch.created_by}"
        logger.warning(msg)
        return {'message': msg}

    try:
        if this_batch.context_data is None:
            # parse bcl convert output and get all output locations
            bcl_convert_output_obj = liborca.parse_bcl_convert_output(this_workflow.output)
            # build a sample info and its related fastq locations
            fastq_list_rows: List = fastq_list_row.handler({
                'fastq_list_rows': bcl_convert_output_obj,
                'seq_name': this_sqr.name,
            }, None)

            # cache batch context data in db
            this_batch = batch_srv.update_batch(this_batch.id, context_data=fastq_list_rows)

            # Initialise fastq list rows object in model
            for row in fastq_list_rows:
                fastq_srv.create_or_update_fastq_list_row(row, this_sqr)

        # Get samplesheet and run files from bcl run
        samplesheet = get_ct_tso_samplesheet_from_bcl_convert_output(this_workflow.output)
        run_info_xml, run_parameters_xml = get_run_xml_files_from_bcl_convert_workflow(this_workflow.input)

        # prepare job list and dispatch to job queue
        job_list = prepare_dragen_tso_ctdna_jobs(this_batch, this_batch_run, this_sqr, samplesheet, run_info_xml, run_parameters_xml)
        if job_list:
            queue_arn = libssm.get_ssm_param(SQS_DRAGEN_TSO_CTDNA_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
        else:
            batch_srv.reset_batch_run(this_batch_run.id)  # reset running if job_list is empty

    except Exception as e:
        batch_srv.reset_batch_run(this_batch_run.id)  # reset running
        raise e

    return {
        'batch_id': this_batch.id,
        'batch_name': this_batch.name,
        'batch_created_by': this_batch.created_by,
        'batch_run_id': this_batch_run.id,
        'batch_run_step': this_batch_run.step,
        'batch_run_status': "RUNNING" if this_batch_run.running else "NOT_RUNNING"
    }


def prepare_dragen_tso_ctdna_jobs(this_batch: Batch, this_batch_run: BatchRun, this_sqr: SequenceRun, samplesheet, run_info_xml, run_parameters_xml) -> List[dict]:
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

    See OrchestratorIntegrationTests.test_prepare_ct_tso_jobs() for example job list of SEQ-II validation run.

    :param this_batch:
    :param this_batch_run:
    :param this_sqr:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(this_batch.context_data)

    # iterate through each sample group by rglb
    for rglb, rglb_df in pd.DataFrame(fastq_list_rows).groupby("rglb"):
        # Check rgsm is identical
        # .item() will raise error if there exists more than one sample name for a given library
        rgsm = rglb_df['rgsm'].unique().item()
        # Sample ID
        samplesheet_sample_id = rglb_df['rgid'].apply(lambda x: x.rsplit(".", 1)[-1]).unique().item()
        # Get the metadata for the library
        # NOTE: this will use the library base ID (i.e. without topup/rerun extension), as the metadata is the same
        lib_metadata: LabMetadata = LabMetadata.objects.get(library_id=rglb)
        # make sure we have recognised sample (e.g. not undetermined)
        if not lib_metadata:
            logger.error(f"SKIP CT TSO workflow for {rgsm}_{rglb}. No metadata for {rglb}, this should not happen!")
            continue

        # skip negative control samples
        if lib_metadata.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower():
            logger.info(f"SKIP CT TSO workflow for '{rgsm}_{rglb}'. Negative-control.")
            continue

        # Skip samples where metadata workflow is set to manual
        if lib_metadata.workflow.lower() == LabMetadataWorkflow.MANUAL.value.lower():
            # We do not pursue manual samples
            logger.info(f"SKIP CT TSO workflow for '{rgsm}_{rglb}'. Workflow set to manual.")
            continue

        # skip if assay is not CT_TSO and type is not CT_DNA
        if not (lib_metadata.type.lower() == LabMetadataType.CT_DNA.value.lower() and
                lib_metadata.assay.lower() == LabMetadataAssay.CT_TSO.value.lower()):
            logger.warning(f"SKIP ctTSO workflow for '{rgsm}_{rglb}'. "
                           f"type: 'ctDNA' != '{lib_metadata.type}' or assay: 'ctTSO' != '{lib_metadata.assay}'")
            continue

        # convert read_1 and read_2 to cwl file location dict format

        rglb_df["read_1"] = rglb_df["read_1"].apply(lambda x: liborca.cwl_file_path_as_string_to_dict(x))
        rglb_df["read_2"] = rglb_df["read_2"].apply(lambda x: liborca.cwl_file_path_as_string_to_dict(x))

        job = {
            "tso500_sample": {
                "sample_id": f"{samplesheet_sample_id}",  # This must match the sample sheet
                "sample_name": f"{rgsm}",
                "sample_type": "DNA",
                "pair_id": f"{rgsm}"
            },
            "fastq_list_rows": rglb_df.to_json(orient="records"),
            "samplesheet": liborca.cwl_file_path_as_string_to_dict(samplesheet),
            "run_info_xml": liborca.cwl_file_path_as_string_to_dict(run_info_xml),
            "run_parameters_xml": liborca.cwl_file_path_as_string_to_dict(run_parameters_xml),
            "seq_run_id": this_sqr.run_id if this_sqr else None,
            "seq_name": this_sqr.name if this_sqr else None,
            "batch_run_id": int(this_batch_run.id)
        }

        job_list.append(job)

    return job_list


def get_ct_tso_samplesheet_from_bcl_convert_output(workflow_output):
    """
    Get the gds path containing the samplesheet used for splitting ctTSO samples
    """

    workflow_output = json.loads(workflow_output)

    samplesheet_locations = [Path(samplesheet.get("location"))
                             for samplesheet in workflow_output['split_sheets']]

    for samplesheet_location in samplesheet_locations:
        regex_obj = re.fullmatch(SAMPLESHEET_ASSAY_TYPE_REGEX, samplesheet_location.name)
        if regex_obj is None:
            logger.warning(f"Could not get SampleSheet '{samplesheet_location.name}' to fit"
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

    return str(Path(bcl_input_dir) / "RunInfo.xml"), str(Path(bcl_input_dir) / "RunParameters.xml")



