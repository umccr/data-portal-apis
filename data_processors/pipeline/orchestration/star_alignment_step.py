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
from data_processors.pipeline.domain.config import SQS_STAR_ALIGNMENT_QUEUE_ARN
from data_processors.pipeline.domain.workflow import ExternalWorkflowHelper, WorkflowType
from data_processors.pipeline.services import workflow_srv, fastq_srv, metadata_srv, libraryrun_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    # todo implement
    #  0)
    #  this_workflow is an instance of succeeded wts_alignment_qc Workflow from database
    #  we won't need to use Batcher
    #  integration pattern:
    #    create correspondant SQS Q, star_alignment.py processing consumer Lambda (i.e. follow existing pattern)
    #         see https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal/pipeline
    #  1)
    #    retrieve FATSQs from WTS QC workflow
    #  2)
    #  see Star Alignment payload for preparing job JSON structure
    #  https://github.com/umccr/nextflow-stack/pull/29
    #  e.g.

    # This workflow has to be of type "wts_alignment_qc"
    if this_workflow.type_name != "wts_alignment_qc":
        logger.error(f"Wrong workflow type {this_workflow.type_name} for {this_workflow.wfr_id}, expected 'wts_alignment_qc'.")
        return {}

    labmeta_list: List[LabMetadata] = workflow_srv.get_labmetadata_by_wfr_id(this_workflow.wfr_id)
    meta_list, libs = metadata_srv.get_wts_metadata_by_wts_qc_runs([this_workflow])
    assert len(labmeta_list) == len(meta_list)
    assert len(labmeta_list) == 1
    labmeta: LabMetadata = labmeta_list[0]
    meta = meta_list[0]
    lib_id = labmeta.library_id
    assert lib_id == meta.library_id

    subject_id = labmeta.subject_id

    job = prepare_star_alignment_job(lib_id, subject_id, this_workflow.sequence_run)

    logger.info(f"Submitting Star-Alignment job for {lib_id}.")
    queue_arn = libssm.get_ssm_param(SQS_STAR_ALIGNMENT_QUEUE_ARN)
    libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=[job])

    # todo finally, couple with few unittest on those functions implemented

    return {
        "subject_id": subject_id,
        "LibraryID": lib_id,
        "Job": job
    }


def prepare_star_alignment_job(library_id, subject_id, sqr: SequenceRun) -> dict:
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

    :param library_id: the library to process
    :param subject_id:
    :param sqr:
    :return: the dict holding the job parameters
    """

    # retreive FastqListRow for library (and sequencing run to avoid picking up older/incorrect data)
    fastq_list_rows: List[FastqListRow] = fastq_srv.get_fastq_list_row_by_rglb_and_sequence_run(library_id, sqr)
    assert len(fastq_list_rows) == 1  # We expect a single record
    fastq_list_row = fastq_list_rows[0]

    helper = ExternalWorkflowHelper(WorkflowType.STAR_ALIGNMENT)

    assert fastq_list_row.rglb == library_id
    return {
        "portal_run_id": helper.get_portal_run_id(),
        "subject_id": subject_id,
        "sample_id": fastq_list_row.rgsm,
        "library_id": library_id,
        "fastq_fwd": fastq_list_row.read_1,
        "fastq_rev": fastq_list_row.read_2,
    }
