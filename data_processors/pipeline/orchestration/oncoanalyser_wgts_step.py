# -*- coding: utf-8 -*-
"""star_alignment_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging

from libumccr.aws import libssm, libsqs

from data_portal.models import Workflow, FastqListRow, LabMetadata, SequenceRun
from data_processors.pipeline.domain.config import SQS_ONCOANALYSER_WGTS_QUEUE_ARN
from data_processors.pipeline.domain.workflow import ExternalWorkflowHelper, WorkflowType
from data_processors.pipeline.services import workflow_srv, fastq_srv, metadata_srv, libraryrun_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    """

    :param this_workflow: by convention this should be oncoanalyser wgs or wts workflow with succeeded status
    :return: a dict with the subject and job payload for a oncoanalyser (wgts) call
    """
    logger.info(f"Processing {this_workflow.type_name} workflow {this_workflow.portal_run_id}")
    if this_workflow.type_name not in [WorkflowType.ONCOANALYSER_WGS.value, WorkflowType.ONCOANALYSER_WTS.value]:
        logger.error(f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, "
                     f"expected '{WorkflowType.ONCOANALYSER_WGS.value}' or '{WorkflowType.ONCOANALYSER_WTS.value}'.")
        return {}

    job = prepare_oncoanalyser_wgts_job(this_workflow)

    logger.info(f"Submitting {WorkflowType.ONCOANALYSER_WGTS.value} job based on workflow {this_workflow.portal_run_id}.")
    queue_arn = libssm.get_ssm_param(SQS_ONCOANALYSER_WGTS_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    return {
        "subject_id": job['subject_id'],
        "Job": job
    }


def prepare_oncoanalyser_wgts_job(workflow: Workflow) -> dict:
    """
    Prepare a Oncoanalyser (WGTS) job JSON to be submitted to SQS.
    {
        "subject_id": "SBJ00910",
        "tumor_wgs_sample_id": "PRJ230001",
        "tumor_wgs_library_id": "L2300001",
        "tumor_wgs_bam": "gds://path/to/wgs_tumor.bam",
        "tumor_wts_sample_id": "MDX210176",
        "tumor_wts_library_id": "L2100746",
        "tumor_wts_bam": "s3://path/to/tumor.bam",
        "normal_wgs_sample_id": "PRJ230003",
        "normal_wgs_library_id": "L2300003",
        "normal_wgs_bam": "gds://path/to/wgs_normal.bam",
        "existing_wgs_dir": "s3://path/to/oncoanalyser/wgs/dir/",
        "existing_wts_dir": "s3://path/to/oncoanalyser/wts/dir/",
    }

    :param workflow: the workflow to use to generate the job
    :return: the dict holding the job parameters
    """

    # NOTE: we can get all the payload parameters from the preceding oncoanalyser wgs/wts workflow inputs,
    #       but we have to find the matching workflows given the current one

    if workflow.type_name == WorkflowType.ONCOANALYSER_WGS.value:
        wgs_wf = workflow
        wts_wf = find_wts_wf(workflow)
        if not wts_wf:
            logger.info(f"Cloud not find matching Oncoanalyser WTS workflow for {workflow.portal_run_id}. Aborting.")
            return {}
        wgs_input = json.loads(wgs_wf.input)
        wts_input = json.loads(wts_wf.input)
    elif workflow.type_name == WorkflowType.ONCOANALYSER_WTS.value:
        wgs_wf = find_wgs_wf(workflow)
        wts_wf = workflow
        if not wgs_wf:
            logger.info(f"Cloud not find matching Oncoanalyser WGS workflow for {workflow.portal_run_id}. Aborting.")
            return {}
        wgs_input = json.loads(wgs_wf.input)
        wts_input = json.loads(wts_wf.input)
    else:
        raise ValueError(f"Wrong input workflow ({workflow.type_name}) for {WorkflowType.ONCOANALYSER_WGTS.value}!")

    subject_id = wgs_input['subject_id']
    tumor_wgs_sample_id = wgs_input['tumor_wgs_sample_id']
    tumor_wgs_library_id = wgs_input['tumor_wgs_library_id']
    tumor_wgs_bam = wgs_input['tumor_wgs_bam']
    normal_wgs_sample_id = wgs_input['normal_wgs_sample_id']
    normal_wgs_library_id = wgs_input['normal_wgs_library_id']
    normal_wgs_bam = wgs_input['normal_wgs_bam']

    tumor_wts_sample_id = wts_input['tumor_wts_sample_id']
    tumor_wts_library_id = wts_input['tumor_wts_library_id']
    tumor_wts_bam = wts_input['tumor_wts_bam']

    existing_wgs_dir = get_existing_wgs_dir(wgs_wf)
    existing_wts_dir = get_existing_wts_dir(wts_wf)

    payload = {
        "subject_id": subject_id,
        "tumor_wgs_sample_id": tumor_wgs_sample_id,
        "tumor_wgs_library_id": tumor_wgs_library_id,
        "tumor_wgs_bam": tumor_wgs_bam,
        "tumor_wts_sample_id": tumor_wts_sample_id,
        "tumor_wts_library_id": tumor_wts_library_id,
        "tumor_wts_bam": tumor_wts_bam,
        "normal_wgs_sample_id": normal_wgs_sample_id,
        "normal_wgs_library_id": normal_wgs_library_id,
        "normal_wgs_bam": normal_wgs_bam,
        "existing_wgs_dir": existing_wgs_dir,
        "existing_wts_dir": existing_wts_dir,
    }

    logger.info(f"Created {WorkflowType.ONCOANALYSER_WGTS.value} paylaod:")
    logger.info(json.dumps(payload))
    return payload


def find_wts_wf(workflow: Workflow) -> Workflow:
    input = json.loads(workflow.input)
    subject = input['subject_id']
    wts_wf_list = workflow_srv.get_workflows_by_subject_id_and_workflow_type(
        subject_id=subject, workflow_type=WorkflowType.ONCOANALYSER_WTS)

    if len(wts_wf_list) < 1:
        # if the OncoAnalyser WTS workflow has not run/finished yet, we end up here
        # we abort processing as it will be picked up once the OA WTS workflow completes
        return None
    elif len(wts_wf_list) == 1:  # this should be the normal case if there are already results
        return wts_wf_list[0]
    else:
        # TODO: check and select the appropriate one (if there is)
        logger.info(f"Can't handle multiple workflows at this time!")
        return None


def find_wgs_wf(workflow: Workflow) -> Workflow:
    input = json.loads(workflow.input)
    subject = input['subject_id']
    wgs_wf_list = workflow_srv.get_workflows_by_subject_id_and_workflow_type(
        subject_id=subject, workflow_type=WorkflowType.ONCOANALYSER_WGS)

    if len(wgs_wf_list) < 1:
        # if the OncoAnalyser WGS workflow has not run/finished yet, we end up here
        # we abort processing as it will be picked up once the OA WGS workflow completes
        return None
    elif len(wgs_wf_list) == 1:  # this should be the normal case if there are already results
        return wgs_wf_list[0]
    else:
        # TODO: check and select the appropriate one (if there is)
        logger.info(f"Can't handle multiple workflows at this time!")
        return None


def get_existing_wgs_dir(workflow: Workflow) -> str:
    # Building wgs directory based on convention defined here:
    #  https://github.com/umccr/nextflow-stack/issues/4#issuecomment-1558383262
    # TODO: check whether we can retrieve that path from the workflow output or elsewhere
    wgs_input = json.loads(workflow.input)
    subject_id = wgs_input['subject_id']
    tumor_wgs_library_id = wgs_input['tumor_wgs_library_id']
    normal_wgs_library_id = wgs_input['normal_wgs_library_id']

    # s3://org.umccr.data.oncoanalyser/analysis_data/<subject>/oncoanalyser/<portal_id>/wgs/<tumor_wgs_id>__<normal_wgs_id>/
    return f"s3://org.umccr.data.oncoanalyser/analysis_data/{subject_id}/oncoanalyser/{workflow.portal_run_id}/wgs/{tumor_wgs_library_id}__{normal_wgs_library_id}/"


def get_existing_wts_dir(workflow: Workflow) -> str:
    # Building wgs directory based on convention defined here:
    #  https://github.com/umccr/nextflow-stack/issues/4#issuecomment-1558383262
    # TODO: check whether we can retrieve that path from the workflow output or elsewhere
    wts_input = json.loads(workflow.input)
    subject_id = wts_input['subject_id']
    tumor_wts_library_id = wts_input['tumor_wts_library_id']

    # s3://org.umccr.data.oncoanalyser/analysis_data/<subject>/oncoanalyser/<portal_id>/wts/<tumor_wts_id>/
    return f"s3://org.umccr.data.oncoanalyser/analysis_data/{subject_id}/oncoanalyser/{workflow.portal_run_id}/wts/{tumor_wts_library_id}/"
