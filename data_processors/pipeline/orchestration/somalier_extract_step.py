# -*- coding: utf-8 -*-
"""somalier_extract_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List, Dict

from libumccr.aws import libssm, libsqs

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_SOMALIER_EXTRACT_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):

    # prepare job list and dispatch to job queue
    job_list = prepare_somalier_extract_jobs(this_workflow)

    if job_list:
        libsqs.dispatch_jobs(queue_arn=libssm.get_ssm_param(SQS_SOMALIER_EXTRACT_QUEUE_ARN), job_list=job_list)
    else:
        logger.warning(f"Calling to prepare_somalier_extract_jobs() return empty list, no job to dispatch...")

    return {
        "somalier_extract_step": job_list
    }


def prepare_somalier_extract_jobs(this_workflow: Workflow) -> List[Dict]:
    """
    TL;DR is if there is 1 dragen wgs qc or wts workflow, there will be one somalier extraction step workflow
    :param this_workflow:
    :return:
    """

    job_list = []
    gds_bam_path = None

    if this_workflow.type_name.lower() == WorkflowType.DRAGEN_WTS.value.lower():
        gds_bam_path = liborca.parse_transcriptome_output_for_bam_file(this_workflow.output)

    elif this_workflow.type_name.lower() == WorkflowType.DRAGEN_WGS_QC.value.lower():
        gds_bam_path = liborca.parse_wgs_alignment_qc_output_for_bam_file(this_workflow.output)

    if gds_bam_path is not None:
        job_list.append({
            "gds_path": gds_bam_path,
        })

    return job_list
