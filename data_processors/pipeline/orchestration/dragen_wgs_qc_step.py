# -*- coding: utf-8 -*-
"""wgs_qc_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List

import pandas as pd

from data_portal.models import LabMetadata, LabMetadataPhenotype, LabMetadataWorkflow, LabMetadataType, Workflow
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.config import SQS_DRAGEN_WGS_QC_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import batch_srv, fastq_srv
from utils import libssm, libsqs, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):

    batcher = Batcher(
        workflow=this_workflow,
        run_step=WorkflowType.DRAGEN_WGS_QC.value.upper(),
        batch_srv=batch_srv,
        fastq_srv=fastq_srv,
        logger=logger,
    )

    if batcher.batch_run is None:
        return batcher.get_skip_message()

    # prepare job list and dispatch to job queue
    job_list = prepare_dragen_wgs_qc_jobs(batcher)
    if job_list:
        libsqs.dispatch_jobs(
            queue_arn=libssm.get_ssm_param(SQS_DRAGEN_WGS_QC_QUEUE_ARN),
            job_list=job_list
        )
    else:
        batcher.reset_batch_run()  # reset running if job_list is empty

    return batcher.get_status()


def prepare_dragen_wgs_qc_jobs(batcher: Batcher) -> List[dict]:
    """
    NOTE: as of DRAGEN_WGS_QC CWL workflow version 3.7.5--1.3.5, it uses fastq_list_rows format
    See Example IAP Run > Inputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.5cc28c147e4e4dfa9e418523188aacec/3.7.5--1.3.5.md

    DRAGEN_WGS_QC job preparation is at _pure_ Library level aggregate.
    Here "Pure" Library ID means without having _topup(N) or _rerun(N) suffixes.
    The fastq_list_row lambda already stripped these topup/rerun suffixes (i.e. what is in this_batch.context_data cache).
    Therefore, it aggregates all fastq list at
        - per sequence run by per library for
            - all different lane(s)
            - all topup(s)
            - all rerun(s)
    This constitute one DRAGEN_WGS_QC job (i.e. one DRAGEN_WGS_QC workflow run).

    See test_prepare_dragen_wgs_qc_jobs() integration test for example job list of SEQ-II validation run.

    :param batcher:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(batcher.batch.context_data)

    # iterate through each sample group by rglb
    for rglb, rglb_df in pd.DataFrame(fastq_list_rows).groupby("rglb"):
        # Check rgsm is identical
        # .item() will raise error if there exists more than one sample name for a given library
        rgsm = rglb_df['rgsm'].unique().item()

        # Get the metadata for the library
        # NOTE: this will use the library base ID (i.e. without topup/rerun extension), as the metadata is the same
        lib_metadata: LabMetadata = LabMetadata.objects.get(library_id=rglb)
        # make sure we have recognised sample (e.g. not undetermined)
        if not lib_metadata:
            logger.error(
                f"SKIP DRAGEN_WGS_QC workflow for {rgsm}_{rglb}. No metadata for {rglb}, this should not happen!"
            )
            continue

        # skip negative control samples
        if lib_metadata.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower():
            logger.info(f"SKIP DRAGEN_WGS_QC workflow for '{rgsm}_{rglb}'. Negative-control.")
            continue

        # Skip samples where metadata workflow is set to manual
        if lib_metadata.workflow.lower() == LabMetadataWorkflow.MANUAL.value.lower():
            # We do not pursue manual samples
            logger.info(f"SKIP DRAGEN_WGS_QC workflow for '{rgsm}_{rglb}'. Workflow set to manual.")
            continue

        # skip DRAGEN_WGS_QC if assay type is not WGS
        if lib_metadata.type.lower() != LabMetadataType.WGS.value.lower():
            logger.warning(f"SKIP DRAGEN_WGS_QC workflow for '{rgsm}_{rglb}'. 'WGS' != '{lib_metadata.type}'.")
            continue

        job = {
            "library_id": f"{rglb}",
            "fastq_list_rows": rglb_df.to_dict(orient="records"),
            "seq_run_id": batcher.sqr.run_id if batcher.sqr else None,
            "seq_name": batcher.sqr.name if batcher.sqr else None,
            "batch_run_id": int(batcher.batch_run.id)
        }

        job_list.append(job)

    return job_list
