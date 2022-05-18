# -*- coding: utf-8 -*-
"""somalier_extract module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List, Dict

from libumccr.aws import libssm, libsqs

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_SOMALIER_EXTRACT_QUEUE_ARN
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

    # FIXME - do I need to return anything?
    return None


def prepare_somalier_extract_jobs(this_workflow: Workflow) -> List[Dict]:
    """
    TL;DR is if there is 1 dragen wgs qc or wts workflow, there will be one somalier extraction step workflow
    :param this_workflow:
    :return:
    """

    # Get the dragen somatic output directory location
    gds_bam_path = liborca.parse_bam_file_from_dragen_output(this_workflow.output)

    job = {
        "gds_path": gds_bam_path,
    }

    return [job]

