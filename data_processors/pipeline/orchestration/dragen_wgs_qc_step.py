# -*- coding: utf-8 -*-
"""wgs_qc_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List

import pandas as pd
from libumccr import libjson
from libumccr.aws import libssm, libsqs

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.batch import Batcher, BatchRule, BatchRuleError
from data_processors.pipeline.domain.config import SQS_DRAGEN_WGS_QC_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType, LabMetadataRule, LabMetadataRuleError
from data_processors.pipeline.services import batch_srv, fastq_srv, metadata_srv, libraryrun_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    logger.info(f"Preparing {WorkflowType.DRAGEN_WGTS_QC.value} workflows")

    batcher = Batcher(
        workflow=this_workflow,
        run_step=WorkflowType.DRAGEN_WGTS_QC.value,
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
            # Used for both WGS and WTS
            queue_arn=libssm.get_ssm_param(SQS_DRAGEN_WGS_QC_QUEUE_ARN),
            job_list=job_list
        )
    else:
        batcher.reset_batch_run()  # reset running if job_list is empty

    return batcher.get_status()


def prepare_dragen_wgs_qc_jobs(batcher: Batcher) -> List[dict]:
    """
    See test_prepare_dragen_wgs_qc_jobs() integration test for example job list

    :param batcher:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(batcher.batch.context_data)

    # iterate through each sample group by rglb and lane
    for grouped_element, grouped_df in pd.DataFrame(fastq_list_rows).groupby(["rglb", "lane"]):

        rglb, lane = grouped_element

        # Check rgsm is identical
        # .item() will raise error if there exists more than one sample name for a given library
        rgsm = grouped_df['rgsm'].unique().item()

        # Get the metadata for the library
        # NOTE: this will use the library base ID (i.e. without topup/rerun extension), as the metadata is the same
        this_metadata: LabMetadata = metadata_srv.get_metadata_by_library_id(rglb)

        try:
            LabMetadataRule(this_metadata) \
                .must_set_workflow() \
                .must_not_manual() \
                .must_not_bcl() \
                .must_be_wgts()

            BatchRule(
                batcher=batcher,
                this_library=str(rglb),
                libraryrun_srv=libraryrun_srv
            ).must_not_have_succeeded_runs()

        except LabMetadataRuleError as me:
            logger.warning(f"SKIP {WorkflowType.DRAGEN_WGTS_QC.value} workflow for '{rgsm}_{rglb}' in lane {lane}. {me}")
            continue

        except BatchRuleError as be:
            logger.warning(f"SKIP {be}")
            continue

        # Update read 1 and read 2 strings to cwl file paths
        grouped_df["read_1"] = grouped_df["read_1"].apply(liborca.cwl_file_path_as_string_to_dict)
        grouped_df["read_2"] = grouped_df["read_2"].apply(liborca.cwl_file_path_as_string_to_dict)

        job = {
            "library_id": f"{rglb}",
            "lane": int(lane),
            "fastq_list_rows": grouped_df.to_dict(orient="records"),
            "seq_run_id": batcher.sqr.run_id if batcher.sqr else None,
            "seq_name": batcher.sqr.name if batcher.sqr else None,
            "batch_run_id": int(batcher.batch_run.id)
        }

        job_list.append(job)

    return job_list
