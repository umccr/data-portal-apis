# -*- coding: utf-8 -*-
"""wts_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List, Dict

import pandas as pd
from libumccr.aws import libssm, libsqs

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_DRAGEN_WTS_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.orchestration import _reduce_and_transform_to_df, _extract_unique_subjects, _handle_rerun
from data_processors.pipeline.services import workflow_srv, metadata_srv, fastq_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    logger.info(f"Preparing {WorkflowType.DRAGEN_WTS.value} workflows")

    this_sqr = this_workflow.sequence_run

    # Check if all other DRAGEN_WTS_QC workflows for this run have finished
    # If yes we continue to the DRAGEN WTS Workflow
    # If not we wait (until all DRAGEN_WTS_QC workflows for this run have finished
    running: List[Workflow] = workflow_srv.get_running_by_sequence_run(
        sequence_run=this_sqr,
        workflow_type=WorkflowType.DRAGEN_WTS_QC
    )
    succeeded: List[Workflow] = workflow_srv.get_succeeded_by_sequence_run(
        sequence_run=this_sqr,
        workflow_type=WorkflowType.DRAGEN_WTS_QC
    )

    subjects = list()
    submitting_subjects = list()
    if len(running) == 0:
        logger.info("All QC workflows finished, proceeding to dragen wts preparation")

        meta_list, run_libraries = metadata_srv.get_wts_metadata_by_wts_qc_runs(succeeded)

        if not meta_list:
            logger.warning(f"No dragen wts metadata found for given run libraries: {run_libraries}")

        job_list, subjects, submitting_subjects = prepare_dragen_wts_jobs(meta_list)

        if job_list:
            logger.info(f"Submitting {len(job_list)} WTS jobs for {submitting_subjects}.")
            queue_arn = libssm.get_ssm_param(SQS_DRAGEN_WTS_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
        else:
            logger.warning(f"Calling to prepare_tumor_normal_jobs() return empty list, no job to dispatch...")
    else:
        logger.warning(f"DRAGEN_WTS_QC workflow finished, but {len(running)} still running. Wait for them to finish...")

    return {
        "subjects": subjects,
        "submitting_subjects": submitting_subjects
    }


def prepare_dragen_wts_jobs(meta_list: List[LabMetadata]) -> (List, List, List):
    """
    NOTE: like TN Workflow dragen wts now uses the metadata list format
    See ICA catalogue
    https://github.com/umccr/cwl-ica/blob/main/.github/catalogue/docs/workflows/dragen-transcriptome-pipeline/3.9.3/dragen-transcriptome-pipeline__3.9.3.md

    DRAGEN_WTS job preparation is at _pure_ Library level.
    Here "Pure" Library ID means we don't need to worry about _topup(N) or _rerun(N) suffixes.

    :param meta_list:
    :return: job_list, subjects, submitting_subjects
    """
    job_list = list()
    submitting_subjects = list()

    # step 1 and 3
    if not meta_list:
        return [], [], []

    meta_list_df = _reduce_and_transform_to_df(meta_list)

    if meta_list_df.shape[0] == 0:
        return [], [], []

    subjects = _extract_unique_subjects(meta_list_df)

    logger.info(f"Preparing Dragen WTS for subjects {subjects}")

    # iterate through each sample group by the library id
    # we use the drop_duplicates so that if there are libraries split across multiple lanes,
    # the get_metadata_by_library_id will collect this.
    for index, row in meta_list_df[["subject_id", "library_id"]].drop_duplicates().iterrows():

        fastq_list_rows = fastq_srv.get_fastq_list_row_by_rglb(row.library_id)

        # Check library id for rerun
        fastq_list_rows = _handle_rerun(fastq_list_rows, row.library_id)

        job_list.append(create_wts_job(fastq_list_rows, subject_id=row.subject_id, library_id=row.library_id))
        submitting_subjects.append(job_list)

    return job_list, subjects, submitting_subjects


def create_wts_job(fastq_list_rows: List[FastqListRow], subject_id: str, library_id: str) -> Dict:
    # Get fastq list rows into dict format
    fqlr = pd.DataFrame([fq_list_row.to_dict() for fq_list_row in fastq_list_rows]).to_dict(orient="records")

    # create WTS job definition
    job_dict = {
        "subject_id": subject_id,
        "library_id": library_id,
        "fastq_list_rows": fqlr
    }

    return job_dict
