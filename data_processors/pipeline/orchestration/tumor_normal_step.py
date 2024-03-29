# -*- coding: utf-8 -*-
"""tumor_normal_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List
import json

import pandas as pd
from libumccr.aws import libssm, libsqs

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata, LabMetadataWorkflow
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_TN_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.orchestration import _reduce_and_transform_to_df, _extract_unique_subjects, \
    _mint_libraries
from data_processors.pipeline.services import workflow_srv, metadata_srv, fastq_srv
from data_processors.pipeline.tools import liborca
from data_processors.pipeline.orchestration import _handle_rerun

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow):
    this_sqr = this_workflow.sequence_run

    # check if all other DRAGEN_WGS_QC workflows for this run have finished
    # if yes we continue to the T/N workflow
    # if not, we wait (until all DRAGEN_WGS_QC workflows have finished)
    running: List[Workflow] = workflow_srv.get_running_by_sequence_run(
        sequence_run=this_sqr,
        workflow_type=WorkflowType.DRAGEN_WGS_QC
    )
    succeeded: List[Workflow] = workflow_srv.get_succeeded_by_sequence_run(
        sequence_run=this_sqr,
        workflow_type=WorkflowType.DRAGEN_WGS_QC
    )

    subjects = list()
    submitting_subjects = list()
    if len(running) == 0:
        logger.info("All QC workflows finished, proceeding to T/N preparation")

        meta_list, run_libraries = metadata_srv.get_tn_metadata_by_qc_runs(succeeded)
        if not meta_list:
            logger.warning(f"No T/N metadata found for given run libraries: {run_libraries}")

        job_list, subjects, submitting_subjects = prepare_tumor_normal_jobs(meta_list)

        if job_list:
            logger.info(f"Submitting {len(job_list)} T/N jobs for {submitting_subjects}.")
            queue_arn = libssm.get_ssm_param(SQS_TN_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
        else:
            logger.warning(f"Calling to prepare_tumor_normal_jobs() return empty list, no job to dispatch...")
    else:
        logger.warning(f"DRAGEN_WGS_QC workflow finished, but {len(running)} still running. Wait for them to finish...")

    return {
        "subjects": subjects,
        "submitting_subjects": submitting_subjects
    }


def _is_tn_in_run(meta_list_df, subject_libraries_stripped):
    l_set = set(meta_list_df["library_id"].apply(liborca.strip_topup_rerun_from_library_id).unique().tolist())
    r_set = set(subject_libraries_stripped)
    return False if len(l_set.intersection(r_set)) == 0 else True


def prepare_tumor_normal_jobs(meta_list: List[LabMetadata]) -> (List, List, List):
    """
    See https://github.com/umccr/data-portal-apis/pull/262 for T/N paring algorithm
    Here we note the step from above algorithm as we go through in comment

    If you need debug locally for this routine, see test_tumor_normal_step.py

    Oh, btw, you need "CCR Greatest Hits" playlist in the background, if you go through this! -victor

    :param meta_list:
    :return: job_list, subjects, submitting_subjects
    """
    job_list = list()
    submitting_subjects = list()

    # step 1 and 3
    if not meta_list:
        return [], [], []

    meta_list_df = _reduce_and_transform_to_df(meta_list)
    subjects = _extract_unique_subjects(meta_list_df)

    logger.info(f"Preparing T/N workflows for subjects {subjects}")

    # step 2
    for subject in subjects:

        subject_tn_fqlr_pairs: List[tuple] = []

        # step 4 - iterate over meta-workflow aggregate as we don't want to match clinical with research samples
        # ...sort of
        # We are happy for a research tumor to match with a clinical normal
        # But we do NOT want a clinical tumor to match with a research normal
        for workflow, meta_list_by_wfl_df in meta_list_df.groupby("workflow"):
            # step 5
            # get all libraries (includes 'topup' and 'rerun' suffix) for this subject, phenotype, type and workflow
            # also includes libraries from other runs
            subject_normal_libraries: List[str] = metadata_srv.get_wgs_normal_libraries_by_subject(
                subject_id=subject,
                meta_workflow=str(workflow)
            )

            subject_tumor_libraries: List[str] = metadata_srv.get_wgs_tumor_libraries_by_subject(
                subject_id=subject,
                meta_workflow=str(workflow)
            )

            # ---
            # We have _some_ normal and tumor for this subject from metadata store.
            # Let perform deep strip searches against this run's meta_list_df

            # Strip topup / rerun normal from all of these libraries
            # if this run contains a topup or rerun, we will reprocess
            subject_normal_libraries_stripped = _mint_libraries(subject_normal_libraries)
            subject_tumor_libraries_stripped = _mint_libraries(subject_tumor_libraries)

            # Set booleans for how we go about creating our T/N pairs
            # Is a normal for this subject / library combo in this run?
            normal_in_run = _is_tn_in_run(meta_list_df, subject_normal_libraries_stripped)
            # Is a tumor for this subject / library combo in this run?
            tumor_in_run = _is_tn_in_run(meta_list_df, subject_tumor_libraries_stripped)

            # step 6c - sanity check if normal is in this run, do same for tumor, must be at least one yes
            if not normal_in_run and not tumor_in_run:
                # Don't really know how we got here
                continue

            # Make exception for research tumor and clinical normal
            if tumor_in_run and len(subject_normal_libraries_stripped) == 0 and workflow == LabMetadataWorkflow.RESEARCH.value:
                # Re collect clinical normal libraries
                subject_normal_libraries: List[str] = metadata_srv.get_wgs_normal_libraries_by_subject(
                    subject_id=subject,
                    meta_workflow=str(LabMetadataWorkflow.CLINICAL.value)
                )
                subject_normal_libraries_stripped = _mint_libraries(subject_normal_libraries)
                if len(subject_normal_libraries_stripped) > 0:
                    logger.info(f"Pairing research tumors(s) in run '{', '.join(subject_tumor_libraries_stripped)}' "
                                f"with clinical normal(s) '{', '.join(subject_normal_libraries_stripped)}'")
            if normal_in_run and len(subject_tumor_libraries_stripped) == 0 and workflow == LabMetadataWorkflow.CLINICAL.value:
                # Re collect research tumor libraries
                subject_tumor_libraries: List[str] = metadata_srv.get_wgs_tumor_libraries_by_subject(
                    subject_id=subject,
                    meta_workflow=str(LabMetadataWorkflow.RESEARCH.value)
                )
                subject_tumor_libraries_stripped = _mint_libraries(subject_tumor_libraries)
                if len(subject_tumor_libraries_stripped) > 0:
                    logger.info(f"Pairing clinical normal(s) in run '{', '.join(subject_normal_libraries_stripped)}' "
                                f"with research tumor(s) '{', '.join(subject_tumor_libraries_stripped)}'")

            # step 6a
            # if the normal libraries are empty skip
            if len(subject_normal_libraries) == 0:
                logging.warning(f"Skipping, since we can't find a normal for this subject {subject}")
                continue

            # step 6b
            # if the tumor libraries are empty skip
            if len(subject_tumor_libraries) == 0:
                logging.warning(f"Skipping, since we can't find a tumor for this subject {subject}")
                continue

            # step 7a - check only one normal exists
            if not len(list(set(subject_normal_libraries_stripped))) == 1:
                logger.warning(f"We have multiple normals {subject_normal_libraries_stripped} for this "
                               f"{subject}/{workflow}. Skipping!")
                continue

            # step 7b - check subject has no pending QC workflow running across sequencing (See issue #475)
            subject_qc_across_sequencing: list[Workflow] = workflow_srv.get_workflows_by_subject_id_and_workflow_type(
                subject_id=subject,
                workflow_type=WorkflowType.DRAGEN_WGS_QC,
                workflow_status=WorkflowStatus.RUNNING,
                library_ids=subject_tumor_libraries_stripped + subject_normal_libraries_stripped
            )
            if len(subject_qc_across_sequencing) > 0:
                logger.warning(f"We still have QC workflow running for this {subject}/{workflow}. Skipping!")
                continue

            # ---
            # So far so good! Now let gather FASTQs from FastqListRow store for corresponding libraries

            # step 8 - set the normal library id and get the fastq list rows from it!
            normal_library_id = subject_normal_libraries_stripped[0]
            normal_fastq_list_rows = fastq_srv.get_fastq_list_row_by_rglb(normal_library_id)

            # FIXME - skip if normal library id contains rerun
            normal_fastq_list_rows = _handle_rerun(normal_fastq_list_rows, normal_library_id)

            # step 8a - check to make sure fastq list rows exist
            # there could be library from older run; circa before Portal introducing FastqListRow model and capturing
            if len(normal_fastq_list_rows) == 0:
                logger.warning(f"Thought we had a normal with but we don't have any matching FastqListRow "
                               f"for normal library {normal_library_id} for this {subject}/{workflow}. Skipping!")
                continue

            # step 9 - set tumor FastqListRow(s) that should be per library
            # If we're here because there is a normal in the run, iterate over all tumors we have
            # includes the ones in this run AND those in previous runs
            if normal_in_run:
                for tumor_library_id in subject_tumor_libraries_stripped:
                    tumor_fastq_list_rows = fastq_srv.get_fastq_list_row_by_rglb(tumor_library_id)

                    # FIXME - skip if tumor library id contains rerun
                    tumor_fastq_list_rows = _handle_rerun(tumor_fastq_list_rows, tumor_library_id)

                    if not len(tumor_fastq_list_rows) == 0:
                        subject_tn_fqlr_pairs.append((tumor_fastq_list_rows, normal_fastq_list_rows))
                    else:
                        logger.warning(f"Thought we had T/N pair with {tumor_library_id}/{normal_library_id} but"
                                       f"could not find any FastqListRow(s) for {tumor_library_id} for this "
                                       f"{subject}/{workflow}. Skipping!")

            # step 10
            else:
                # Just the tumor(s) in this run; pre-existing tumors will have been analysed
                tumors = meta_list_df["library_id"].apply(liborca.strip_topup_rerun_from_library_id).unique().tolist()
                for tumor_library_id in tumors:
                    if tumor_library_id not in subject_tumor_libraries_stripped:
                        continue
                    tumor_fastq_list_rows = fastq_srv.get_fastq_list_row_by_rglb(tumor_library_id)
                    if not len(tumor_fastq_list_rows) == 0:
                        subject_tn_fqlr_pairs.append((tumor_fastq_list_rows, normal_fastq_list_rows))
                    else:
                        logger.warning(f"Thought we had T/N pair with {tumor_library_id}/{normal_library_id} but"
                                       f"could not find any FastqListRow(s) for {tumor_library_id} for this "
                                       f"{subject}/{workflow}. Skipping!")

        for (tumor_fastq_list_rows, normal_fastq_list_rows) in subject_tn_fqlr_pairs:
            job_list.append(create_tn_job(tumor_fastq_list_rows, normal_fastq_list_rows, subject_id=subject))
            submitting_subjects.append(subject)

    # Get unique list of job list
    # In the event that if a clinical normal and research tumor are on the same run
    # Job list
    # Get a unique list of dicts
    # https://stackoverflow.com/a/11092607/6946787
    job_list = list(
        map(
            lambda x: json.loads(x),
            set(
                map(
                    lambda x: json.dumps(x),
                    job_list
                )
            )
        )
    )

    # Get unique list of submitting subjects
    submitting_subjects = list(set(submitting_subjects))

    return job_list, subjects, submitting_subjects


def create_tn_job(tumor_fastq_list_rows: List[FastqListRow], normal_fastq_list_rows: List[FastqListRow], subject_id):
    # Get tumor sample name
    tumor_library_id = tumor_fastq_list_rows[0].rglb
    tumor_sample_id = tumor_fastq_list_rows[0].rgsm

    normal_library_id = normal_fastq_list_rows[0].rglb
    normal_sample_id = normal_fastq_list_rows[0].rgsm

    fqlr = pd.DataFrame([fq_list_row.to_dict() for fq_list_row in normal_fastq_list_rows]).to_dict(orient="records")
    t_fqlr = pd.DataFrame([fq_list_row.to_dict() for fq_list_row in tumor_fastq_list_rows]).to_dict(orient="records")

    # create T/N job definition
    job_dict = {
        "subject_id": subject_id,
        "fastq_list_rows": fqlr,
        "tumor_fastq_list_rows": t_fqlr,
        "output_file_prefix_germline": f"{normal_sample_id}",
        "output_file_prefix_somatic": f"{tumor_sample_id}",
        "output_directory_germline": f"{normal_library_id}",
        "output_directory_somatic": f"{tumor_library_id}_{normal_library_id}",
        "sample_name_germline": normal_library_id,
        "sample_name_somatic": tumor_library_id
    }

    return job_dict
