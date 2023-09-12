# -*- coding: utf-8 -*-
"""star_alignment_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
from typing import List

from libumccr.aws import libssm, libsqs

from data_portal.fields import IdHelper
from data_portal.models import Workflow, FastqListRow, LabMetadata, SequenceRun
from data_processors.pipeline.domain.config import SQS_SASH_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import workflow_srv, fastq_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    """

    :param this_workflow: by convention this should be 'oncoanalyser_wgs' workflow with succeeded status
    :return: a dict with the input expected by sash
     """

    # This workflow has to be of type "oncoanalyser_wgs"
    if this_workflow.type_name != WorkflowType.ONCOANALYSER_WGS.value:
        logger.error(f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, expected {WorkflowType.ONCOANALYSER_WGS.value}.")
        return {}

    job = prepare_sash_job(this_workflow)

    logger.info(f"Submitting Sash job for {this_workflow.portal_run_id} ({this_workflow.wfr_name}).")
    queue_arn = libssm.get_ssm_param(SQS_SASH_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    return {
        "Job": job
    }


def prepare_sash_job(this_workflow: Workflow) -> dict:
    """
    Prepare a Star Alignment job JSON to be submitted to SQS.
    See: Star Alignment payload for preparing job JSON structure
    https://github.com/umccr/nextflow-stack/pull/29

    payload = {
        "subject_id": "SBJ00001",
        "tumor_sample_id": "PRJ230001",
        "tumor_library_id": "L2300001",
        "normal_sample_id": "PRJ230002",
        "normal_library_id": "L2300002",
        "dragen_somatic_dir": "gds://production/analysis_data/SBJ00001/wgs_tumor_normal/20230515zyxwvuts/L2300001_L2300002/",
        "dragen_germline_dir": "gds://production/analysis_data/SBJ00001/wgs_tumor_normal/20230515zyxwvuts/L2300002_dragen_germline/",
        "oncoanalyser_dir": "s3://org.umccr.data.oncoanalyser/analysis_data/SBJ00001/oncoanalyser/20230518poiuytre/wgs/L2300001__L2300002/SBJ00001_PRJ230001/"
    }

    :param this_workflow: create a sash job based on this workflow
    :return: the dict holding the job parameters
    """

    # oncoanalyser_wgs takes T/N output as its input, so we can parse
    # oncoanalyser_wgs input to get the portal_run_id of the T/N workflow
    # as no dedicated field exists for the portal_run_id, we have to parse it out of a file path
    onco_input = json.loads(this_workflow.input)
    bam_path = onco_input['tumor_wgs_bam']
    tn_portal_run_id = IdHelper.get_portal_run_id_from_path_element(bam_path)

    tn_workflow = workflow_srv.get_workflow_by_portal_run_id(tn_portal_run_id)

    # get the output directories of the T/N workflow and oncoanalyser_wgs
    dragen_somatic_directory = liborca.parse_somatic_workflow_output_directory(tn_workflow.output)
    dragen_germline_directory = liborca.parse_germline_workflow_output_directory(tn_workflow.output)
    oncoanalyser_output_dir = liborca.parse_workflow_output(this_workflow.output, ['output_directory'])

    # get sample/lib ids from oncoanalyser_wgs input
    subject_id = onco_input['subject_id'],
    tumor_sample_id = onco_input['tumor_wgs_sample_id'],
    tumor_library_id = onco_input['tumor_wgs_library_id'],
    normal_sample_id = onco_input['normal_wgs_sample_id'],
    normal_library_id = onco_input['normal_wgs_library_id'],
    dragen_somatic_dir = dragen_somatic_directory
    dragen_germline_dir = dragen_germline_directory
    oncoanalyser_dir = oncoanalyser_output_dir

    return {
        "subject_id": subject_id,
        "tumor_sample_id": tumor_sample_id,
        "tumor_library_id": tumor_library_id,
        "normal_sample_id": normal_sample_id,
        "normal_library_id": normal_library_id,
        "dragen_somatic_dir": dragen_somatic_dir,
        "dragen_germline_dir": dragen_germline_dir,
        "oncoanalyser_dir": oncoanalyser_dir
    }
