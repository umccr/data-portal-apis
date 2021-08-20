# -*- coding: utf-8 -*-
"""tumor_normal_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from collections import defaultdict
from typing import List
from itertools import product

import pandas as pd

from data_portal.models import Workflow, LabMetadata, LabMetadataType, LabMetadataPhenotype, FastqListRow, LabMetadataWorkflow
from data_processors.pipeline.domain.config import SQS_TN_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import workflow_srv, metadata_srv, fastq_srv
from utils import libssm, libsqs

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get the libraries for this subject that were in this run
PHENOTYPES = [LabMetadataPhenotype.TUMOR, LabMetadataPhenotype.NORMAL]
TYPES = [LabMetadataType.WGS]
WORKFLOWS = [LabMetadataWorkflow.CLINICAL, LabMetadataWorkflow.RESEARCH]


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
    if len(running) == 0:
        logger.info("All QC workflows finished, proceeding to T/N preparation")
        # determine which samples are available for T/N wokflow
        subjects = metadata_srv.get_subjects_from_runs(succeeded)
        libraries = metadata_srv.get_libraries_from_runs(succeeded)
        logger.info(f"Preparing T/N workflows for subjects {subjects}")
        job_list = prepare_tumor_normal_jobs(subjects=subjects, libraries=libraries)
        if job_list:
            logger.info(F"Submitting {len(job_list)} T/N jobs.")
            queue_arn = libssm.get_ssm_param(SQS_TN_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
    else:
        logger.debug(f"DRAGEN_WGS_QC workflow finished, but {len(running)} still running. Wait for them to finish...")

    return {
        "subjects": subjects
    }


def prepare_tumor_normal_jobs(subjects: List[str], libraries: List[str]) -> list:
    jobs = list()
    for subject in subjects:
        # Apply more stringent filtering to our libraries for this subject
        # Were they part of the "clinical" or "research" workflow type
        # Are they a WGS run?
        # Are they of the phenotype 'tumor' or 'normal'?
        sub_lib_rows: List[LabMetadata] = []

        for library_id, subject_id, phenotype, _type, workflow in product(libraries, subjects, PHENOTYPES, TYPES, WORKFLOWS):
            sub_lib_rows.extend(LabMetadata.objects.filter(library_id__exact=library_id,
                                                           subject_id__exact=subject_id,
                                                           phenotype__exact=phenotype,
                                                           type__exact=_type,
                                                           workflow__exact=workflow))

        # Get library rows
        libs_lab_df: pd.DataFrame = pd.DataFrame([{"library_id": lab_metadata.library_id,
                                                   "subject_id": lab_metadata.subject_id,
                                                   "phenotype": lab_metadata.phenotype,
                                                   "type": lab_metadata.type,
                                                   "workflow": lab_metadata.workflow}
                                                  for lab_metadata in sub_lib_rows])

        # Check we have any rows left
        if libs_lab_df.shape[0] == 0:
            # No rows left
            logger.debug(f"Skipping subject {subject} for T/N")
            continue

        # Split by workflow type, find matching tumor or normal
        workflow: str
        sub_lib_by_workflow_df: pd.DataFrame

        subject_t_n_fastq_list_row_pairs: List[tuple] = []

        for workflow, sub_lib_by_workflow_df in libs_lab_df.groupby("workflow"):
            # Get unique values for this dataframe
            subject_id = sub_lib_by_workflow_df["subject_id"].unique().item()

            # All libraries (includes 'topup' and 'rerun') suffix for this given subject, phenotype, type and workflow
            # (includes libraries on other runs)
            all_subject_tumor_libraries: List[str] = LabMetadata.objects.filter(subject_id__exact=subject_id, phenotype="tumor", type="WGS", workflow=workflow)
            all_subject_normal_libraries: List[str] = LabMetadata.objects.filter(subject_id__exact=subject_id, phenotype="normal", type="WGS", workflow=workflow)

            # Strip topup / rerun normal from all of these libraries
            # if this run contains a topup or rerun, we will reprocess
            all_subject_normal_libraries_stripped = list(set([metadata_srv.strip_topup_rerun_from_library_id(lib_id) for lib_id in all_subject_normal_libraries]))
            all_subject_tumor_libraries_stripped = list(set([metadata_srv.strip_topup_rerun_from_library_id(lib_id) for lib_id in all_subject_tumor_libraries]))

            # Set booleans for how we go about creating our T/N pairs
            # Is a tumor for this subject / library combo in this run?
            tumor_in_run = False if len(set(libs_lab_df["library_id"].apply(metadata_srv.strip_topup_rerun_from_library_id).unique().tolist()).
                intersection(set(all_subject_normal_libraries_stripped))) == 0 \
                else True
            # Is a normal for this subject / library combo in this run?
            normal_in_run = False if len(set(libs_lab_df["library_id"].apply(metadata_srv.strip_topup_rerun_from_library_id).unique().tolist()).
                intersection(set(all_subject_tumor_libraries_stripped))) == 0 \
                else True

            # Sanity check
            if not tumor_in_run and not normal_in_run:
                # Don't really know how we got here
                continue

            # Let's check that there's only one normal for this subject, type workflow combo
            # Check if 'normal' in unique phenotypes and check history
            # Then set fastq list rows if all good
            if len(all_subject_normal_libraries) == 0:
                logging.debug(f"Skipping, since we can't find a normal for this subject {subject}")
                continue
            if len(all_subject_tumor_libraries) == 0:
                logging.debug(f"Skipping, since we can't find a tumor for this subject {subject}")
                continue

            # Check only one normal exists
            if not len(list(set(all_subject_normal_libraries_stripped))) == 1:
                logger.warning(f"We have multiple 'normals' for this subject / workflow: '{subject}/{workflow}'. "
                               f"Skipping!")
                continue

            # Set the normal library id and get the fastq list rows from it!
            normal_library_id = all_subject_normal_libraries_stripped[0]
            normal_fastq_list_rows = FastqListRow.objects.filter(rglb__exact=normal_library_id)
            # TODO - remove rows before rerun

            # Make sure fastq list rows exist (might not have been demuxed yet?)
            if len(normal_fastq_list_rows) == 0:
                logger.warning(f"Thought we had a normal with but we don't have any matching fastqs for normal library {normal_library_id}")
                continue

            # Set tumor fastq list rows (should be per library)
            # If we're here because theres a normal in the run, iterate over all tumors we have
            # includes the ones in this run AND those in previous runs
            if normal_in_run:
                for tumor_library_id in all_subject_tumor_libraries_stripped:
                    tumor_fastq_list_rows = FastqListRow.objects.filter(rglb__exact=tumor_library_id)
                    # TODO - remove rows before rerun
                    if not len(tumor_fastq_list_rows) == 0:
                        subject_t_n_fastq_list_row_pairs.append((tumor_fastq_list_rows, normal_fastq_list_rows))
                    else:
                        logger.warning(f"Thought we had t/n pair with {tumor_library_id}/{normal_library_id} but"
                                       f"could not find any fastq list rows for {tumor_library_id}")
            else:
                # Just the tumor(s) in this run
                # Pre-existing tumors will have been analysed
                for tumor_library_id in libs_lab_df["library_id"].apply(metadata_srv.strip_topup_rerun_from_library_id).unique().tolist():
                    tumor_fastq_list_rows = FastqListRow.objects.filter(rglb__exact=tumor_library_id)
                    if not len(tumor_fastq_list_rows) == 0:
                        subject_t_n_fastq_list_row_pairs.append((tumor_fastq_list_rows, normal_fastq_list_rows))
                    else:
                        logger.warning(f"Thought we had t/n pair with {tumor_library_id}/{normal_library_id} but"
                                       f"could not find any fastq list rows for {tumor_library_id}")

        for (tumor_fastq_list_rows, normal_fastq_list_rows) in subject_t_n_fastq_list_row_pairs:
            jobs.extend(create_tn_job(tumor_fastq_list_rows, normal_fastq_list_rows, subject_id=subject))

    return jobs


def create_tn_job(tumor_fastq_list_rows: List[FastqListRow], normal_fastq_list_rows: List[FastqListRow], subject_id):
    # Get tumor sample name
    tumor_sample_id = tumor_fastq_list_rows[0].rgsm

    # create T/N job definition
    job_json = {
        "subject_id": subject_id,
        "fastq_list_rows": pd.DataFrame([fq_list_row.to_dict() for fq_list_row in normal_fastq_list_rows]).to_json(orient="records"),
        "tumor_fastq_list_rows": pd.DataFrame([fq_list_row.to_dict() for fq_list_row in tumor_fastq_list_rows]).to_json(orient="records"),
        "output_file_prefix": tumor_sample_id,
        "output_directory": subject_id,
        "sample_name": tumor_sample_id
    }

    return job_json