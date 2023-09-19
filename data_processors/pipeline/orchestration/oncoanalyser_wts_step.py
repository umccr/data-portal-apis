# -*- coding: utf-8 -*-
"""star_alignment_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
from typing import List

from libumccr.aws import libssm, libsqs

from data_portal.models import Workflow, FastqListRow, LabMetadata, SequenceRun
from data_processors.pipeline.domain.config import SQS_ONCOANALYSER_WTS_QUEUE_ARN
from data_processors.pipeline.domain.workflow import ExternalWorkflowHelper, WorkflowType
from data_processors.pipeline.services import workflow_srv, fastq_srv, metadata_srv, libraryrun_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    """

    :param this_workflow: by convention this should be 'wgs_tumor_normal' workflow with succeeded status
    :return: a dict with the subject and job payload for a oncoanalyser (wts) call
    """

    # This workflow has to be of type "star_alignment"
    if this_workflow.type_name != WorkflowType.STAR_ALIGNMENT.value:
        logger.error(f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, expected '{WorkflowType.STAR_ALIGNMENT.value}'.")
        return {}

    job = prepare_oncoanalyser_wts_job(this_workflow)

    logger.info(f"Submitting {WorkflowType.TUMOR_NORMAL.value} job based on workflow {this_workflow.portal_run_id}.")
    queue_arn = libssm.get_ssm_param(SQS_ONCOANALYSER_WTS_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    return {
        "subject_id": job['subject_id'],
        "Job": job
    }


def prepare_oncoanalyser_wts_job(workflow: Workflow) -> dict:
    """
    Prepare a Oncoanalyser (WTS) job JSON to be submitted to SQS.
    {
        "subject_id": "SBJ00910",
        "tumor_wts_sample_id": "MDX210176",
        "tumor_wts_library_id": "L2100746",
        "tumor_wts_bam": "path/to/tumor.bam",
    }

    :param workflow: the workflow to use to generate the job
    :return: the dict holding the job parameters
    """

    # NOTE: we can get the subject, sample and library ids from the input to the star alignment job
    #       the tumor bam location we'll have to construct using a conventions shared with the star alignment job
    #       We may be able to "look up" the location by querying the file store

    # Get the BAM location from the Star alignment output
    star_input = json.loads(workflow.input)
    logger.info(f"FOO: {star_input}")
    subject_id = star_input['subject_id']
    sample_id = star_input['sample_id']
    library_id = star_input['library_id']
    tumor_wts_bam = construct_bam_location(workflow.portal_run_id, subject_id, sample_id, library_id)

    payload = {
        "subject_id": subject_id,
        "tumor_wts_sample_id": sample_id,
        "tumor_wts_library_id": library_id,
        "tumor_wts_bam": tumor_wts_bam
    }

    logger.info(f"Created {WorkflowType.TUMOR_NORMAL.value} paylaod:")
    logger.info(json.dumps(payload))
    return payload


def construct_bam_location(portal_run_id: str, subject_id: str, sample_id: str, library_id: str) -> str:
    # TODO check and pull bucket name/prefix from SSM so it reflects deployment env
    # s3://org.umccr.data.oncoanalyser/analysis_data/SBJ02102/star-align-nf/2023091822e2eb7a/L2200544/PRJ221057/PRJ221057.md.bam
    return f"s3://org.umccr.data.oncoanalyser/analysis_data/{subject_id}/star-align-nf/{portal_run_id}/{library_id}/{sample_id}/{sample_id}.md.bam"
