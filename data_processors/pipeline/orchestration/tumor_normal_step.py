# -*- coding: utf-8 -*-
"""tumor_normal_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List

import pandas as pd

from data_portal.models import Workflow, LabMetadata, LabMetadataType, LabMetadataPhenotype, FastqListRow
from data_processors.pipeline.domain.config import SQS_TN_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import workflow_srv, metadata_srv, fastq_srv
from data_processors.pipeline.tools import liborca
from utils import libssm, libsqs

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

        job_list, subjects, submitting_subjects = prepare_tumor_normal_jobs(succeeded)

        if job_list:
            logger.info(f"Submitting {len(job_list)} T/N jobs for {submitting_subjects}.")
            queue_arn = libssm.get_ssm_param(SQS_TN_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
        else:
            logger.warning(f"Calling to prepare_tumor_normal_jobs() return empty list, no job to dispatch...")
    else:
        logger.debug(f"DRAGEN_WGS_QC workflow finished, but {len(running)} still running. Wait for them to finish...")

    return {
        "subjects": subjects,
        "submitting_subjects": submitting_subjects
    }


def _reduce_and_transform_to_df(meta_list: List[LabMetadata]) -> pd.DataFrame:
    # also reduce to columns of interest
    return pd.DataFrame(
        [
            {
                "library_id": meta.library_id,
                "subject_id": meta.subject_id,
                "phenotype": meta.phenotype,
                "type": meta.type,
                "workflow": meta.workflow
            } for meta in meta_list
        ]
    )


def _extract_unique_subjects(meta_list_df: pd.DataFrame) -> List[str]:
    return meta_list_df["subject_id"].unique().tolist()


def _mint_libraries(libraries):
    s = set()
    for lib in libraries:
        s.add(liborca.strip_topup_rerun_from_library_id(lib))
    return list(s)


def _is_tn_in_run(meta_list_df, subject_libraries_stripped):
    l_set = set(meta_list_df["library_id"].apply(liborca.strip_topup_rerun_from_library_id).unique().tolist())
    r_set = set(subject_libraries_stripped)
    return False if len(l_set.intersection(r_set)) == 0 else True


def _handle_rerun(fastq_list_rows: List[FastqListRow], library_id):
    for fastq_list_row in fastq_list_rows:
        full_sample_library_id = fastq_list_row.rgid.rsplit(".", 1)[-1]
        if liborca.sample_library_id_has_rerun(full_sample_library_id):
            logger.warning(f"We found a rerun for library id {library_id} in {full_sample_library_id}. "
                           f"Please run this sample manually")
            # Reset fastq list rows - we don't know what to do with reruns
            return []
    return fastq_list_rows


def prepare_tumor_normal_jobs(succeeded_qc_workflows: List[Workflow]) -> (List, List, List):
    """
    See https://github.com/umccr/data-portal-apis/pull/262 for T/N paring algorithm
    Here we note the step from above algorithm as we go through in comment

    If you need debug locally for this routine, see tn_debug.py

    Oh, btw, you need "CCR Greatest Hits" playlist in the background, if you go through this! -victor

    :param succeeded_qc_workflows:
    :return: job_list, subject_list
    """
    job_list = list()
    submitting_subjects = list()

    # step 1 and 3
    meta_list, run_libraries = metadata_srv.get_tn_metadata_by_qc_runs(succeeded_qc_workflows)
    if not meta_list:
        logger.warning(f"No T/N metadata found for given run libraries: {run_libraries}")
        return [], [], []

    meta_list_df = _reduce_and_transform_to_df(meta_list)

    subjects = _extract_unique_subjects(meta_list_df)
    logger.info(f"Preparing T/N workflows for subjects {subjects}")

    # step 2
    for subject in subjects:

        subject_tn_fqlr_pairs: List[tuple] = []

        # step 4 - iterate over meta-workflow aggregate as we don't want to match clinical with research samples
        for workflow, meta_list_by_wfl_df in meta_list_df.groupby("workflow"):

            # step 5
            # get all libraries (includes 'topup' and 'rerun' suffix) for this subject, phenotype, type and workflow
            # also includes libraries from other runs
            subject_normal_libraries: List[str] = metadata_srv.get_all_libraries_by_keywords(
                subject_id=subject,
                phenotype=LabMetadataPhenotype.NORMAL.value,
                type_=LabMetadataType.WGS.value,
                meta_wfl=workflow
            )

            # step 6a
            if len(subject_normal_libraries) == 0:
                logging.debug(f"Skipping, since we can't find a normal for this subject {subject}")
                continue

            subject_tumor_libraries: List[str] = metadata_srv.get_all_libraries_by_keywords(
                subject_id=subject,
                phenotype=LabMetadataPhenotype.TUMOR.value,
                type_=LabMetadataType.WGS.value,
                meta_wfl=workflow
            )

            # step 6b
            if len(subject_tumor_libraries) == 0:
                logging.debug(f"Skipping, since we can't find a tumor for this subject {subject}")
                continue

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

            # step 7 - check only one normal exists
            if not len(list(set(subject_normal_libraries_stripped))) == 1:
                logger.warning(f"We have multiple normals {subject_normal_libraries_stripped} for this "
                               f"{subject}/{workflow}. Skipping!")
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
            # If we're here because theres a normal in the run, iterate over all tumors we have
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

    return job_list, subjects, submitting_subjects


def create_tn_job(tumor_fastq_list_rows: List[FastqListRow], normal_fastq_list_rows: List[FastqListRow], subject_id):
    # Get tumor sample name
    tumor_sample_id = tumor_fastq_list_rows[0].rgsm
    fqlr = pd.DataFrame([fq_list_row.to_dict() for fq_list_row in normal_fastq_list_rows]).to_dict(orient="records")
    t_fqlr = pd.DataFrame([fq_list_row.to_dict() for fq_list_row in tumor_fastq_list_rows]).to_dict(orient="records")

    # create T/N job definition
    job_dict = {
        "subject_id": subject_id,
        "fastq_list_rows": fqlr,
        "tumor_fastq_list_rows": t_fqlr,
        "output_file_prefix": tumor_sample_id,
        "output_directory": subject_id,
        "sample_name": tumor_sample_id
    }

    return job_dict
