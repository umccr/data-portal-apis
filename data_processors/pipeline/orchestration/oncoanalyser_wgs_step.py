# -*- coding: utf-8 -*-
"""oncoanalyser_wgs_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
from typing import List

from libumccr.aws import libssm, libsqs

from data_portal.models import Workflow, LabMetadata
from data_processors.pipeline.domain.config import SQS_ONCOANALYSER_WGS_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    """

    :param this_workflow: by convention this should be 'wgs_tumor_normal' workflow with succeeded status
    :return: a dict with the subject and job payload for a oncoanalyser (wgs) call
    """

    # This workflow has to be of type "wgs_tumor_normal"
    if this_workflow.type_name != WorkflowType.TUMOR_NORMAL.value:
        logger.error(
            f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, "
            f"expected '{WorkflowType.TUMOR_NORMAL.value}'."
        )
        return {}

    job = prepare_oncoanalyser_wgs_job(this_workflow)

    if not job:
        logger.warning(f"Calling to prepare_oncoanalyser_wgs_job() return empty dict, no job to dispatch...")
        return {}

    logger.info(f"Submitting {WorkflowType.ONCOANALYSER_WGS.value} job induced by "
                f"workflow ({this_workflow.type_name}, {this_workflow.portal_run_id})")

    queue_arn = libssm.get_ssm_param(SQS_ONCOANALYSER_WGS_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    return job


def prepare_oncoanalyser_wgs_job(this_workflow: Workflow) -> dict:
    """
    Prepare a Oncoanalyser (WGS) job JSON to be submitted to SQS.
    {
        "subject_id": "SBJ00910",
        "tumor_wgs_sample_id": "MDX210176",
        "tumor_wgs_library_id": "L2100746",
        "tumor_wgs_bam": "path/to/tumor.bam",
        "normal_wgs_sample_id": "MDX210175",
        "normal_wgs_library_id": "L2100745",
        "normal_wgs_bam": "path/to/normal.bam"
    }

    :param this_workflow: the workflow to use to generate the job
    :return: the dict holding the job parameters
    """

    payload = {
        "subject_id": None,
        "tumor_wgs_sample_id": None,
        "tumor_wgs_library_id": None,
        "tumor_wgs_bam": None,
        "normal_wgs_sample_id": None,
        "normal_wgs_library_id": None,
        "normal_wgs_bam": None
    }

    # Get the BAM locations from the T/N output
    tumor_wgs_bam, normal_wgs_bam = liborca.parse_wgs_tumor_normal_output_for_bam_files(this_workflow.output)

    payload["tumor_wgs_bam"] = tumor_wgs_bam
    payload["normal_wgs_bam"] = normal_wgs_bam

    # Get the rest of the information from the metadata linked to the workflow
    labmeta_list: List[LabMetadata] = workflow_srv.get_labmetadata_by_workflow(this_workflow)

    fill_ids_from_metadata(payload, labmeta_list)

    logger.info(f"Created {WorkflowType.ONCOANALYSER_WGS.value} payload:")
    logger.info(json.dumps(payload))

    return payload


def fill_ids_from_metadata(payload: dict, labmeta_list: List[LabMetadata]):
    # We expect a metadata list of two entries: normal + tumor samples
    if len(labmeta_list) == 2:

        # expected case
        # extract subject ID (and make sure they match)
        assert labmeta_list[0].subject_id == labmeta_list[1].subject_id, ValueError("T/N pair with differing SubjectID")
        subject_id = labmeta_list[0].subject_id

        # extract sample + library IDs
        if labmeta_list[0].phenotype == 'tumor':
            tumor = labmeta_list[0]
            normal = labmeta_list[1]
        else:
            tumor = labmeta_list[1]
            normal = labmeta_list[0]

        assert normal.phenotype == 'normal', ValueError(
            f"Expecting 'normal' library, found '{normal.phenotype}' for {normal.library_id}"
        )

        tumor_wgs_sample_id = tumor.sample_id
        tumor_wgs_library_id = tumor.library_id
        normal_wgs_sample_id = normal.sample_id
        normal_wgs_library_id = normal.library_id
    else:
        # should not happen
        raise ValueError(f"Got {len(labmeta_list)} labmetadata records, but expected 2!")

    payload["subject_id"] = subject_id
    payload["tumor_wgs_sample_id"] = tumor_wgs_sample_id
    payload["tumor_wgs_library_id"] = tumor_wgs_library_id
    payload["normal_wgs_sample_id"] = normal_wgs_sample_id
    payload["normal_wgs_library_id"] = normal_wgs_library_id
