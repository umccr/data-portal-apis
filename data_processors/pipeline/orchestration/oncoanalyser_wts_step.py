# -*- coding: utf-8 -*-
"""oncoanalyser_wts_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging

from libumccr.aws import libssm, libsqs

from data_portal.models import Workflow
from data_processors.pipeline.domain.config import SQS_ONCOANALYSER_WTS_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import s3object_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    """

    :param this_workflow: by convention this should be 'star_alignment' workflow with succeeded status
    :return: a dict with the subject and job payload for a oncoanalyser (wts) call
    """

    # This workflow has to be of type "star_alignment"
    if this_workflow.type_name != WorkflowType.STAR_ALIGNMENT.value:
        logger.error(
            f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, "
            f"expected '{WorkflowType.STAR_ALIGNMENT.value}'."
        )
        return {}

    job = prepare_oncoanalyser_wts_job(this_workflow)

    logger.info(f"Submitting {WorkflowType.ONCOANALYSER_WTS.value} job induced by "
                f"workflow ({this_workflow.type_name}, {this_workflow.portal_run_id})")

    queue_arn = libssm.get_ssm_param(SQS_ONCOANALYSER_WTS_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    return job


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
    star_alignment_input = json.loads(workflow.input)
    subject_id = star_alignment_input['subject_id']
    sample_id = star_alignment_input['sample_id']
    library_id = star_alignment_input['library_id']
    tumor_wts_bam = get_star_alignment_output_bam(portal_run_id=workflow.portal_run_id)

    payload = {
        "subject_id": subject_id,
        "tumor_wts_sample_id": sample_id,
        "tumor_wts_library_id": library_id,
        "tumor_wts_bam": tumor_wts_bam
    }

    logger.info(f"Created {WorkflowType.ONCOANALYSER_WTS.value} payload:")
    logger.info(json.dumps(payload))
    return payload


def get_star_alignment_output_bam(portal_run_id: str):
    """
    Here, we look up Portal S3Object index table for given portal_run_id.
    Alternatively, we could also parse from star alignment Batch event output.
    """

    results = s3object_srv.get_s3_files_for_path_tokens(path_tokens=[
        portal_run_id,
        ".bam",
    ])

    filtered_list = list(filter(lambda x: str(x).endswith(".bam"), results))

    assert len(filtered_list) == 1, ValueError("Multiple or no BAM file found for star_alignment output")

    return filtered_list[0]
