# -*- coding: utf-8 -*-
"""wts_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List

import pandas as pd

from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataWorkflow, LabMetadataType
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.config import SQS_DRAGEN_WTS_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import batch_srv, fastq_srv, metadata_srv
from data_processors.pipeline.tools import liborca
from utils import libssm, libsqs, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    logger.info(f"Preparing {WorkflowType.DRAGEN_WTS.value} workflows")

    batcher = Batcher(
        workflow=this_workflow,
        run_step=WorkflowType.DRAGEN_WTS.value.upper(),
        batch_srv=batch_srv,
        fastq_srv=fastq_srv,
        logger=logger,
    )

    if batcher.batch_run is None:
        return batcher.get_skip_message()

    # prepare job list and dispatch to job queue
    job_list = prepare_dragen_wts_jobs(batcher)
    if job_list:
        libsqs.dispatch_jobs(
            queue_arn=libssm.get_ssm_param(SQS_DRAGEN_WTS_QUEUE_ARN),
            job_list=job_list
        )
    else:
        batcher.reset_batch_run()  # reset running if job_list is empty

    return batcher.get_status()


def prepare_dragen_wts_jobs(batcher: Batcher) -> List[dict]:
    """
    NOTE: like DRAGEN_WGS_QC CWL workflow, DRAGEN_WTS also uses fastq_list_rows format
    See ICA catalogue
    https://github.com/umccr/cwl-ica/blob/main/.github/catalogue/docs/workflows/dragen-transcriptome-pipeline/3.8.4/dragen-transcriptome-pipeline__3.8.4.md

    DRAGEN_WTS job preparation is at _pure_ Library level.
    Here "Pure" Library ID means we don't need to worry about _topup(N) or _rerun(N) suffixes.

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
        meta: LabMetadata = metadata_srv.get_metadata_by_library_id(rglb)
        # make sure we have recognised sample (e.g. not undetermined)
        if meta is None:
            logger.error(f"SKIP DRAGEN_WTS workflow for {rgsm}_{rglb}. "
                         f"No metadata for {rglb}, this should not happen!")
            continue

        # skip negative control samples
        # if meta.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower():
        #     logger.info(f"SKIP DRAGEN_WTS workflow for '{rgsm}_{rglb}'. Negative-control.")
        #     continue

        # Skip samples where metadata workflow is set to manual
        if meta.workflow.lower() == LabMetadataWorkflow.MANUAL.value.lower():
            # We do not pursue manual samples
            logger.info(f"SKIP DRAGEN_WTS workflow for '{rgsm}_{rglb}'. Workflow set to manual.")
            continue

        # skip DRAGEN_WTS workflow if assay type is not WTS
        if meta.type.lower() != LabMetadataType.WTS.value.lower():
            logger.warning(f"SKIP DRAGEN_WTS workflow for '{rgsm}_{rglb}'. 'WTS' != '{meta.type}'.")
            continue

        # Update read 1 and read 2 strings to cwl file paths
        rglb_df["read_1"] = rglb_df["read_1"].apply(liborca.cwl_file_path_as_string_to_dict)
        rglb_df["read_2"] = rglb_df["read_2"].apply(liborca.cwl_file_path_as_string_to_dict)

        job = {
            "library_id": f"{rglb}",
            "fastq_list_rows": rglb_df.to_dict(orient="records"),
            "seq_run_id": batcher.sqr.run_id if batcher.sqr else None,
            "seq_name": batcher.sqr.name if batcher.sqr else None,
            "batch_run_id": int(batcher.batch_run.id)
        }

        job_list.append(job)

    return job_list
