# -*- coding: utf-8 -*-
"""dragen_tso_ctdna_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
from typing import List, Dict

from data_portal.models.workflow import Workflow
from data_portal.models.labmetadata import  LabMetadata
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.config import SQS_UMCCRISE_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import batch_srv, fastq_srv, metadata_srv
from data_processors.pipeline.tools import liborca
from utils import libssm, libsqs

# GLOBALS
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):

    batcher = Batcher(
        workflow=this_workflow,
        run_step=WorkflowType.UMCCRISE.value.upper(),
        batch_srv=batch_srv,
        fastq_srv=fastq_srv,
        logger=logger,
    )

    if batcher.batch_run is None:
        return batcher.get_skip_message()

    # prepare job list and dispatch to job queue
    job_list = prepare_umccrise_jobs(batcher)
    if job_list:
        libsqs.dispatch_jobs(
            queue_arn=libssm.get_ssm_param(SQS_UMCCRISE_QUEUE_ARN),
            job_list=job_list
        )
    else:
        batcher.reset_batch_run()  # reset running if job_list is empty

    return batcher.get_status()


def prepare_umccrise_jobs(batcher: Batcher) -> List[dict]:
    """
    NOTE: This launches the umccrise workflow

    There is a 1:1 ratio between dragen somatic workflows and umccrise workflows.
    This constitute one umccrise job (i.e. one umccrise workflow run).

    :param batcher:
    :return:
    """

    # Get dragen somatic directory and fastq list rows for the germline sample
    # We also collect the fastq list rows for the tumor sample so we can do some metadata checks
    dragen_somatic_directory = get_dragen_somatic_output_directory_from_somatic_workflow_output(batcher.workflow.output)
    fastq_list_rows_germline = get_fastq_list_rows_germline_from_somatic_workflow_input(batcher.workflow.input)
    fastq_list_rows_tumor = get_fastq_list_rows_tumor_from_somatic_workflow_input(batcher.workflow.input)

    # Get normal sample name for naming germline outputs
    normal_rgsm = fastq_list_rows_germline[0].rgsm

    # Get tumor and normal library names for umccrise outputs
    normal_rglb = fastq_list_rows_germline[0].rglb
    tumor_rglb = fastq_list_rows_tumor[0].rglb

    # Get metadata for both tumor and normal libraries
    meta_normal: LabMetadata = metadata_srv.get_metadata_by_library_id(normal_rglb)
    meta_tumor: LabMetadata = metadata_srv.get_metadata_by_library_id(tumor_rglb)

    # Confirm both tumor and normal libraries belong to the same subject
    if not meta_normal.subject_id == meta_tumor.subject_id:
        logger.warning("Normal and tumor do NOT belong to the same subject, skipping sample")
        return None

    # Get subject identifier for umccrise workflow

    job = {
        "dragen_somatic_directory": dragen_somatic_directory,
        "fastq_list_rows_germline": fastq_list_rows_germline,
        "output_directory_germline": normal_rgsm,
        "output_directory_umccrise": f"{tumor_rglb}__{normal_rglb}",
        "output_file_prefix_germline": normal_rgsm,
        "subject_identifier_umccrise": meta_tumor.subject_id
    }

    return [job]


def get_dragen_somatic_output_directory_from_somatic_workflow_output(somatic_workflow_output: str) -> Dict:
    """
    Get the dragen somatic output directory location
    """

    dragen_somatic_output_directory = liborca.parse_somatic_output_workflow_directory(somatic_workflow_output)

    if dragen_somatic_output_directory is None:
        logger.warning("Could not find a dragen somatic output directory from the somatic workflow")
        return None

    return dragen_somatic_output_directory


def get_fastq_list_rows_germline_from_somatic_workflow_input(somatic_workflow_input: str) -> Dict:
    """
    From the input object, get the bcl_input_directory directory value and add 'RunInfo.xml' and 'RunParameters.xml'
    :param somatic_workflow_input:
    :return:
    """

    somatic_workflow_input = json.loads(somatic_workflow_input)

    fastq_list_rows_germline = somatic_workflow_input.get('fastq_list_rows', None)

    if fastq_list_rows_germline is None:
        logger.warning("Could not find input 'fastq_list_rows' from the dragen somatic workflow")
        return None

    return fastq_list_rows_germline


def get_fastq_list_rows_tumor_from_somatic_workflow_input(somatic_workflow_input: str) -> Dict:
    """
    From the input object, get the bcl_input_directory directory value and add 'RunInfo.xml' and 'RunParameters.xml'
    :param somatic_workflow_input:
    :return:
    """

    somatic_workflow_input = json.loads(somatic_workflow_input)

    fastq_list_rows_tumor = somatic_workflow_input.get('tumor_fastq_list_rows', None)

    if fastq_list_rows_tumor is None:
        logger.warning("Could not find input 'tumor_fastq_list_rows' from the dragen somatic workflow")
        return None

    return fastq_list_rows_tumor
