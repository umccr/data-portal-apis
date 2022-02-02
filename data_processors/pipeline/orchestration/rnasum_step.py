# -*- coding: utf-8 -*-
"""rnasum_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import logging
from typing import List, Dict

from libumccr.aws import libssm, libsqs

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_RNASUM_QUEUE_ARN
from data_processors.pipeline.services import metadata_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):

    # prepare job list and dispatch to job queue
    job_list = prepare_rnasum_jobs(this_workflow)
    if job_list:
        libsqs.dispatch_jobs(queue_arn=libssm.get_ssm_param(SQS_RNASUM_QUEUE_ARN), job_list=job_list)
    else:
        logger.warning(f"Calling to prepare_rnasum_jobs() return empty list, no job to dispatch...")

    submitting_subjects = []
    for job in job_list:
        submitting_subjects.append(job['subject_identifier_rnasum'])

    return {
        "submitting_subjects": submitting_subjects
    }


def prepare_rnasum_jobs(this_workflow: Workflow) -> List[Dict]:
    """
    TL;DR is if there is 1 dragen somatic workflow run and 1 dragen WTS workflow run, 
    then there will be 1 umccrise run and 1 rnasum run.

    Basically, there is 1 to 1 between umccrise workflow and rnasum workflow, given WTS
    data for the sample is also being run.

    :param this_workflow:
    :return:
    """

    # Get the dragen transcriptome output directory location - TODO need to figure out 
    # how to parse both transcriptome and umccrise "Workflow"
    dragen_transcriptome_directory = liborca.parse_transcriptome_workflow_output_directory(this_workflow.output)

    # Get the umccrise output directory location
    umccrise_directory = liborca.parse_umccrise_workflow_output_directory(this_workflow.output)

    # Get fastq list rows for the transcriptome sample
    fqlr = get_fastq_list_rows_from_transcriptome_workflow_input(this_workflow.input)
    
    # Get tumor library names for rnasum outputs
    tumor_rglb = fqlr[0]['rglb']
    
    # Get metadata for tumour library
    meta_tumor: LabMetadata = metadata_srv.get_metadata_by_library_id(tumor_rglb)

    subject_identifier = meta_tumor.subject_id 

    # Get patient specific reference dataset via REDCAP? TBD
    tumor_dataset = "TODO"

    job = {
        "dragen_transcriptome_directory": dragen_transcriptome_directory,
        "umccrise_directory": umccrise_directory,
        "sample_name": meta_tumor.sample_id,
        "report_directory": f"{subject_identifier}__{tumor_rglb}",
        "dataset": tumor_dataset
    }

    return [job]

def get_fastq_list_rows_from_transcriptome_workflow_input(transcriptome_workflow_input: str) -> (List[Dict], List[Dict]):
    """
    From the transcriptome (tumor only) workflow input object, get 'fastq_list_rows'

    :param transcriptome_workflow_input:
    :return:
    """

    transcriptome_workflow_input = json.loads(transcriptome_workflow_input)

    fastq_list_rows = transcriptome_workflow_input.get('fastq_list_rows', None)

    if fastq_list_rows is None:
        raise ValueError("Could not find input 'fastq_list_rows' from the dragen transcriptome workflow")

    if not fastq_list_rows:
        raise ValueError("Unexpected transcriptome workflow input. The 'fastq_list_rows' is empty.")

    return fastq_list_rows