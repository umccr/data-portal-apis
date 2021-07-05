# -*- coding: utf-8 -*-
"""tumor_normal_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from collections import defaultdict
from typing import List

from data_portal.models import Workflow, LabMetadata, LabMetadataType, LabMetadataPhenotype, FastqListRow
from data_processors.pipeline.domain.config import SQS_TN_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import workflow_srv, metadata_srv, fastq_srv
from utils import libssm, libsqs

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_sqr):
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
        # determine which samples are available for T/N wokflow
        subjects = metadata_srv.get_subjects_from_runs(succeeded)
        job_list = prepare_tumor_normal_jobs(subjects=subjects)
        if job_list:
            queue_arn = libssm.get_ssm_param(SQS_TN_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
    else:
        logger.debug(f"DRAGEN_WGS_QC workflow finished, but {len(running)} still running. Wait for them to finish...")

    return {
        "subjects": subjects
    }


def prepare_tumor_normal_jobs(subjects: List[str]) -> list:
    jobs = list()
    for subject in subjects:
        jobs.extend(create_tn_jobs(subject))

    return jobs


def create_tn_jobs(subject_id: str) -> list:
    # TODO: could query for all records in one go and then filter locally
    # records = LabMetadata.objects.filter(subject_id=subject_id)

    # extract WGS tumor/normal samples for a subject
    tumor_records: List[LabMetadata] = LabMetadata.objects.filter(
        subject_id=subject_id,
        type__iexact=LabMetadataType.WGS.value.lower(),
        phenotype__iexact=LabMetadataPhenotype.TUMOR.value.lower())
    normal_records: List[LabMetadata]= LabMetadata.objects.filter(
        subject_id=subject_id,
        type__iexact=LabMetadataType.WGS.value.lower(),
        phenotype__iexact=LabMetadataPhenotype.NORMAL.value.lower())

    # TODO: sort out topup/rerun logic

    # check if we have metadata records for both phenotypes (otherwise there's no point for a T/N workflow)
    if len(tumor_records) == 0 or len(normal_records) == 0:
        logger.warning(f"Skipping subject {subject_id} (tumor or normal lib still missing).")
        return list()

    # there should be one tumor and one normal, if there isn't we need to figure out what to do
    # find all tumor FASTQs ordered be sample ID
    t_fastq_list_rows = defaultdict(list)
    for record in tumor_records:
        fastq_rows = FastqListRow.objects.filter(rglb=record.library_id)
        t_fastq_list_rows[record.sample_id].extend(fastq_rows)
    # find all normal FASTQs ordered be sample ID
    n_fastq_list_rows = defaultdict(list)
    for record in normal_records:
        fastq_rows = FastqListRow.objects.filter(rglb=record.library_id)
        n_fastq_list_rows[record.sample_id].extend(fastq_rows)

    if len(t_fastq_list_rows) < 1:
        logger.info(f"Skipping subject {subject_id} (tumor FASTQs still missing).")
        return list()
    if len(n_fastq_list_rows) < 1:
        logger.info(f"Skipping subject {subject_id} (normal FASTQs still missing).")
        return list()
    if len(n_fastq_list_rows) > 1:
        logger.warning(f"Skipping subject {subject_id} (too many normals).")
        return list()

    # at this point we have one normal and at least one tumor
    # we are going to create one job for each tumor (paired to the normal)
    norma_fastq_list_rows = list(n_fastq_list_rows.values())[0]
    job_jsons = list()
    for t_rows in t_fastq_list_rows.values():
        j_json = create_job_json(subject_id=subject_id,
                                 normal_fastq_list_rows=norma_fastq_list_rows,
                                 tumor_fastq_list_rows=t_rows)
        if j_json:
            job_jsons.append(j_json)

    return job_jsons


def create_job_json(subject_id: str, normal_fastq_list_rows: List[FastqListRow], tumor_fastq_list_rows: List[FastqListRow]):
    # quick check: at this point we'd expect one library/sample for each normal/tumor
    # NOTE: IDs are from rglb/rgsm of FastqListRow, so library IDs are stripped of _topup/_rerun extensions
    # TODO: handle other cases (multiple tumor/normal samples)
    # TODO: if more than one tumor (but only one normal) treat as two runs, one for each tumor (using the same normal)
    n_samples, n_libraries = fastq_srv.extract_sample_library_ids(normal_fastq_list_rows)
    logger.info(f"Normal samples/Libraries for subject {subject_id}: {n_samples}/{n_libraries}")
    t_samples, t_libraries = fastq_srv.extract_sample_library_ids(tumor_fastq_list_rows)
    logger.info(f"Tumor samples/Libraries for subject {subject_id}: {t_samples}/{t_libraries}")
    if len(n_samples) != 1 or len(n_libraries) != 1:
        logger.warning(f"Unexpected number of normal samples! Skipping subject {subject_id}")
        return None
    if len(t_samples) != 1 or len(t_libraries) != 1:
        logger.warning(f"Unexpected number of tumor samples! Skipping subject {subject_id}")
        return None

    tumor_sample_id = t_samples[0]

    # hacky way to convert non-serializable Django Model objects to the Json format we expect
    # TODO: find a better way to define a Json Serializer for Django Model objects
    normal_dict_list = list()
    for row in normal_fastq_list_rows:
        normal_dict_list.append(row.to_dict())
    tumor_dict_list = list()
    for row in tumor_fastq_list_rows:
        tumor_dict_list.append(row.to_dict())

    # create T/N job definition
    job_json = {
        "subject_id": subject_id,
        "fastq_list_rows": normal_dict_list,
        "tumor_fastq_list_rows": tumor_dict_list,
        "output_file_prefix": tumor_sample_id,
        "output_directory": subject_id,
        "sample_name": tumor_sample_id
    }

    return job_json
