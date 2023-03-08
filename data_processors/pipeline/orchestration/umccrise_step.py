# -*- coding: utf-8 -*-
"""umccrise_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
from typing import List, Dict

from libumccr.aws import libssm, libsqs

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_UMCCRISE_QUEUE_ARN
from data_processors.pipeline.services import metadata_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):

    # prepare job list and dispatch to job queue
    job_list = prepare_umccrise_jobs(this_workflow)
    if job_list:
        libsqs.dispatch_jobs(queue_arn=libssm.get_ssm_param(SQS_UMCCRISE_QUEUE_ARN), job_list=job_list)
    else:
        logger.warning(f"Calling to prepare_umccrise_jobs() return empty list, no job to dispatch...")

    submitting_subjects = []
    for job in job_list:
        submitting_subjects.append(job['subject_identifier'])

    return {
        "submitting_subjects": submitting_subjects
    }


def prepare_umccrise_jobs(this_workflow: Workflow) -> List[Dict]:
    """
    TL;DR is if there is 1 dragen somatic workflow run then there will be 1 umccrise run.

    Basically, there is 1 to 1 between dragen somatic (wgs_tumor_normal) workflow and umccrise workflow.
    So, much of heavy lifting are already done as part of dragen somatic workflow, such as
      - tumor normal pairing,
      - determining related metadata
      - should there be triggering a dragen somatic workflow run, or not
      etc.
    Therefore, 1 umccrise job is directly depended on this_workflow (wgs_tumor_normal) run.

    :param this_workflow:
    :return:
    """

    # Get the dragen somatic output directory location
    dragen_somatic_directory = liborca.parse_somatic_workflow_output_directory(this_workflow.output)
    dragen_germline_directory = liborca.parse_germline_workflow_output_directory(this_workflow.output)

    # Get fastq list rows for the germline sample
    # We also collect the fastq list rows for the tumor sample, so we can do some metadata checks
    fqlr_germline, fqlr_tumor = get_fastq_list_rows_from_somatic_workflow_input(this_workflow.input)

    # Get tumor and normal library names for umccrise outputs
    normal_rglb = fqlr_germline[0]['rglb']
    normal_rgsm = fqlr_germline[0]['rgsm']

    tumor_rglb = fqlr_tumor[0]['rglb']
    tumor_rgsm = fqlr_tumor[0]['rgsm']

    # Get metadata for both tumor and normal libraries
    meta_normal: LabMetadata = metadata_srv.get_metadata_by_library_id(normal_rglb)
    meta_tumor: LabMetadata = metadata_srv.get_metadata_by_library_id(tumor_rglb)

    # Confirm both tumor and normal libraries belong to the same subject
    if not meta_normal.subject_id == meta_tumor.subject_id:
        logger.warning("Normal and tumor do NOT belong to the same subject, skipping sample")
        return []  # return empty list to effectively skip

    # Get subject identifier for umccrise workflow

    job = {
        "dragen_somatic_directory": dragen_somatic_directory,
        "dragen_germline_directory": dragen_germline_directory,
        "output_directory_name": f"{tumor_rglb}__{normal_rglb}",
        "subject_identifier": meta_tumor.subject_id,
        "sample_name": meta_tumor.sample_id,
        "tumor_library_id": tumor_rglb,
        "normal_library_id": normal_rglb,
        "dragen_tumor_id": tumor_rgsm,
        "dragen_normal_id": normal_rgsm,
    }

    return [job]


def get_fastq_list_rows_from_somatic_workflow_input(somatic_workflow_input: str) -> (List[Dict], List[Dict]):
    """
    From the somatic (tumor normal) workflow input object, get 'fastq_list_rows' and 'tumor_fastq_list_rows'

    :param somatic_workflow_input:
    :return:
    """

    somatic_workflow_input = json.loads(somatic_workflow_input)

    fastq_list_rows_germline = somatic_workflow_input.get('fastq_list_rows', None)

    if fastq_list_rows_germline is None:
        raise ValueError("Could not find input 'fastq_list_rows' from the dragen somatic workflow")

    if not fastq_list_rows_germline:
        raise ValueError("Unexpected somatic workflow input. The 'fastq_list_rows' is empty.")

    fastq_list_rows_tumor = somatic_workflow_input.get('tumor_fastq_list_rows', None)

    if fastq_list_rows_tumor is None:
        raise ValueError("Could not find input 'tumor_fastq_list_rows' from the dragen somatic workflow")

    if not fastq_list_rows_tumor:
        raise ValueError("Unexpected somatic workflow input. The 'tumor_fastq_list_rows' is empty.")

    return fastq_list_rows_germline, fastq_list_rows_tumor
