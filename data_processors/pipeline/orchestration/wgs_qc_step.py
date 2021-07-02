import logging
from typing import List

import pandas as pd

from data_portal.models import Batch, BatchRun, SequenceRun, LabMetadata, LabMetadataPhenotype, LabMetadataWorkflow, \
    LabMetadataType
from data_processors.pipeline import services, constant
from data_processors.pipeline.constant import WorkflowType
from data_processors.pipeline.lambdas import fastq_list_row
from data_processors.pipeline.tools import liborca
from utils import libssm, libsqs, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_sqr, this_workflow):
    # create a batch if not exist
    batch_name = this_sqr.name if this_sqr else f"{this_workflow.type_name}__{this_workflow.wfr_id}"
    this_batch = services.get_or_create_batch(name=batch_name, created_by=this_workflow.wfr_id)

    # register a new batch run for this_batch run step
    this_batch_run = services.skip_or_create_batch_run(
        batch=this_batch,
        run_step=WorkflowType.GERMLINE.value.upper()
    )
    if this_batch_run is None:
        # skip the request if there is on going existing batch_run for the same batch run step
        # this is especially to fence off duplicate IAP WES events hitting multiple time to our IAP event lambda
        msg = f"SKIP. THERE IS EXISTING ON GOING RUN FOR BATCH " \
              f"ID: {this_batch.id}, NAME: {this_batch.name}, CREATED_BY: {this_batch.created_by}"
        logger.warning(msg)
        return {'message': msg}

    try:
        if this_batch.context_data is None:
            # parse bcl convert output and get all output locations
            # build a sample info and its related fastq locations
            fastq_list_rows: List = fastq_list_row.handler({
                'fastq_list_rows': liborca.parse_bcl_convert_output(this_workflow.output),
                'seq_name': this_sqr.name,
            }, None)

            # cache batch context data in db
            this_batch = services.update_batch(this_batch.id, context_data=fastq_list_rows)

            # Initialise fastq list rows object in model
            for row in fastq_list_rows:
                services.create_or_update_fastq_list_row(row, this_sqr)

        # prepare job list and dispatch to job queue
        job_list = prepare_germline_jobs(this_batch, this_batch_run, this_sqr)
        if job_list:
            queue_arn = libssm.get_ssm_param(constant.SQS_GERMLINE_QUEUE_ARN)
            libsqs.dispatch_jobs(queue_arn=queue_arn, job_list=job_list)
        else:
            services.reset_batch_run(this_batch_run.id)  # reset running if job_list is empty

    except Exception as e:
        services.reset_batch_run(this_batch_run.id)  # reset running
        raise e

    return {
        'batch_id': this_batch.id,
        'batch_name': this_batch.name,
        'batch_created_by': this_batch.created_by,
        'batch_run_id': this_batch_run.id,
        'batch_run_step': this_batch_run.step,
        'batch_run_status': "RUNNING" if this_batch_run.running else "NOT_RUNNING"
    }


def prepare_germline_jobs(this_batch: Batch, this_batch_run: BatchRun, this_sqr: SequenceRun) -> List[dict]:
    """
    NOTE: as of GERMLINE CWL workflow version 3.7.5--1.3.5, it uses fastq_list_rows format
    See Example IAP Run > Inputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.5cc28c147e4e4dfa9e418523188aacec/3.7.5--1.3.5.md

    Germline job preparation is at _pure_ Library level aggregate.
    Here "Pure" Library ID means without having _topup(N) or _rerun(N) suffixes.
    The fastq_list_row lambda already stripped these topup/rerun suffixes (i.e. what is in this_batch.context_data cache).
    Therefore, it aggregates all fastq list at
        - per sequence run by per library for
            - all different lane(s)
            - all topup(s)
            - all rerun(s)
    This constitute one Germline job (i.e. one Germline workflow run).

    See OrchestratorIntegrationTests.test_prepare_germline_jobs() for example job list of SEQ-II validation run.

    :param this_batch:
    :param this_batch_run:
    :param this_sqr:
    :return:
    """
    job_list = []
    fastq_list_rows: List[dict] = libjson.loads(this_batch.context_data)

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
            logger.error(f"SKIP GERMLINE workflow for {rgsm}_{rglb}. No metadata for {rglb}, this should not happen!")
            continue

        # skip negative control samples
        if lib_metadata.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower():
            logger.info(f"SKIP GERMLINE workflow for '{rgsm}_{rglb}'. Negative-control.")
            continue

        # Skip samples where metadata workflow is set to manual
        if lib_metadata.workflow.lower() == LabMetadataWorkflow.MANUAL.value.lower():
            # We do not pursue manual samples
            logger.info(f"SKIP GERMLINE workflow for '{rgsm}_{rglb}'. Workflow set to manual.")
            continue

        # skip germline if assay type is not WGS
        if lib_metadata.type.lower() != LabMetadataType.WGS.value.lower():
            logger.warning(f"SKIP GERMLINE workflow for '{rgsm}_{rglb}'. 'WGS' != '{lib_metadata.type}'.")
            continue

        # convert read_1 and read_2 to cwl file location dict format

        rglb_df["read_1"] = rglb_df["read_1"].apply(lambda x: liborca.cwl_file_path_as_string_to_dict(x))
        rglb_df["read_2"] = rglb_df["read_2"].apply(lambda x: liborca.cwl_file_path_as_string_to_dict(x))

        job = {
            "sample_name": f"{rgsm}_{rglb}",
            "fastq_list_rows": rglb_df.to_json(orient="records"),
            "seq_run_id": this_sqr.run_id if this_sqr else None,
            "seq_name": this_sqr.name if this_sqr else None,
            "batch_run_id": int(this_batch_run.id)
        }

        job_list.append(job)

    return job_list
