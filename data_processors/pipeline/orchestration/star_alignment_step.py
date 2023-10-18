# -*- coding: utf-8 -*-
"""star_alignment_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List

from libumccr.aws import libssm, libsqs

from data_portal.models import Workflow, FastqListRow, LabMetadata
from data_processors.pipeline.domain.config import SQS_STAR_ALIGNMENT_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import fastq_srv, metadata_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    """
    See Star Alignment payload for preparing job JSON structure
    https://github.com/umccr/nextflow-stack/pull/29

    :param this_workflow: by convention this should be 'wts_alignment_qc' workflow with succeeded status
    :return: a dict with the subject, library and job payload for a star alignment call
    """

    if this_workflow.type_name != WorkflowType.DRAGEN_WTS_QC.value:
        logger.error(f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, "
                     f"expected 'wts_alignment_qc'")
        return {}

    job = prepare_star_alignment_job(this_workflow)

    if not job:
        logger.warning(f"Calling to prepare_star_alignment_job() return empty dict, no job to dispatch...")
        return {}

    logger.info(f"Submitting Star Alignment job for {job.get('subject_id')}")

    queue_arn = libssm.get_ssm_param(SQS_STAR_ALIGNMENT_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    return job


def prepare_star_alignment_job(this_workflow: Workflow) -> dict:
    """
    Prepare a Star Alignment job JSON to be submitted to SQS.
    See: Star Alignment payload for preparing job JSON structure
    https://github.com/umccr/nextflow-stack/pull/29

    payload = {
        'portal_run_id': '20230530abcdefgh',
        'subject_id': 'SBJ00001',
        'sample_id': 'PRJ230002',
        'library_id': 'L2300002',
        'fastq_fwd': 'gds://production/primary_data/230430_A00001_0001_AH1VLHDSX1/20230430qazwsxed/WTS_NebRNA/PRJ230002_L2300002_S1_L001_R1_001.fastq.gz',
        'fastq_rev': 'gds://production/primary_data/230430_A00001_0001_AH1VLHDSX1/20230430qazwsxed/WTS_NebRNA/PRJ230002_L2300002_S1_L001_R2_001.fastq.gz',
    }

    :param this_workflow: preceding Workflow instance
    :return: the dict holding the job parameters
    """
    sqr = this_workflow.sequence_run

    meta_list, _ = metadata_srv.get_wts_metadata_by_wts_qc_runs([this_workflow])

    if len(meta_list) == 0:
        logger.warning(f"No metadata found for workflow ({this_workflow})")
        return {}
    elif len(meta_list) >= 2:
        logger.warning(f"Multiple metadata found for workflow ({this_workflow})")
        return {}

    meta: LabMetadata = meta_list[0]

    # retrieve FastqListRow for library (and sequencing run to avoid picking up older/incorrect data)
    fastq_list_rows: List[FastqListRow] = fastq_srv.get_fastq_list_row_by_rglb_and_sequence_run(meta.library_id, sqr)

    if len(fastq_list_rows) == 0:
        logger.warning(f"No FastqListRow found for library_id ({meta.library_id})")
        return {}
    elif len(fastq_list_rows) >= 2:
        logger.warning(f"Multiple FastqListRow found for library_id ({meta.library_id})")
        return {}

    # expect a single record
    fastq_list_row = fastq_list_rows[0]

    return {
        "subject_id": meta.subject_id,
        "sample_id": fastq_list_row.rgsm,
        "library_id": meta.library_id,
        "fastq_fwd": fastq_list_row.read_1,
        "fastq_rev": fastq_list_row.read_2,
    }
