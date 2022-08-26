# -*- coding: utf-8 -*-
"""orchestrator module

Orchestrator (lambda) module is the key actor (controller) of Portal Workflow Automation.
Typically, this module has 3 simple interfaces:
    1. handler()        -- for SQS event
    2. update_step()    -- update some workflow
    3. next_step()      -- determine next workflow, if any

See "orchestration" package for _steps_ modules that compliment Genomic workflow core orchestration domain logic.
"""
try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.orchestration import dracarys_multiqc_step, dragen_wgs_qc_step, tumor_normal_step, google_lims_update_step, \
    dragen_tso_ctdna_step, fastq_update_step, dragen_wts_step, umccrise_step, rnasum_step, somalier_extract_step
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus, WorkflowRule
from data_processors.pipeline.lambdas import workflow_update
from libumccr import libjson
from libumccr.aws import libssm

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """event payload dict
    {
        'wfr_id': "wfr.xxx",
        'wfv_id': "wfv.xxx",
        'wfr_event': {
            'event_type': "RunSucceeded",
            'event_details': {},
            'timestamp': "2020-06-24T11:27:35.1268588Z"
        },
        'skip': {
            'global' : [
                "UPDATE_STEP",
                "FASTQ_UPDATE_STEP",
                "GOOGLE_LIMS_UPDATE_STEP",
                "DRAGEN_WGS_QC_STEP",
                "DRAGEN_TSO_CTDNA_STEP",
                "DRAGEN_WTS_STEP",
                "TUMOR_NORMAL_STEP",
                "UMCCRISE_STEP",
                "RNASUM_STEP",
                "SOMALIER_EXTRACT_STEP"
            ],
            'by_run': {
                '220524_A01010_0998_ABCF2HDSYX': [
                    "FASTQ_UPDATE_STEP",
                    "GOOGLE_LIMS_UPDATE_STEP",
                    "DRAGEN_WGS_QC_STEP",
                    "DRAGEN_TSO_CTDNA_STEP",
                    "DRAGEN_WTS_STEP",
                ],
                '220525_A01010_0999_ABCF2HDSYX': [
                    "UPDATE_STEP",
                    "FASTQ_UPDATE_STEP",
                    "GOOGLE_LIMS_UPDATE_STEP",
                    "DRAGEN_WGS_QC_STEP",
                    "DRAGEN_TSO_CTDNA_STEP",
                    "DRAGEN_WTS_STEP",
                ]
            }
        }
    }

    NOTE:
    Orchestrator will read static configuration step_skip_list from SSM param. And merge with skip list from the event.
    For example, to skip DRAGEN_WGS_QC_STEP, simply payload value like as follows. To reset, just payload empty list [].
        aws ssm put-parameter \
          --name "/iap/workflow/step_skip_list" \
          --type "String" \
          --value "{\"global\": [\"DRAGEN_WGS_QC_STEP\"]}" \
          --overwrite \
          --profile dev

    :param event:
    :param context:
    :return: None
    """

    logger.info(f"Start processing workflow orchestrator event")
    logger.info(libjson.dumps(event))

    wfr_id = event['wfr_id']
    wfv_id = event['wfv_id']
    wfr_event = event.get('wfr_event')  # wfr_event is optional
    skip = event.get('skip', dict())    # skip is optional
    if 'global' not in skip:
        skip['global'] = list()
    if 'by_run' not in skip:
        skip['by_run'] = dict()

    try:
        ssm_skip_json = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/step_skip_list")
        ssm_skip = libjson.loads(ssm_skip_json)
    except Exception as e:
        # If any exception found, log warning and proceed
        logger.warning(f"Cannot read step_skip_list from SSM param. Exception: {e}")
        ssm_skip = {}
    skip['global'].extend(ssm_skip.get('global', []))
    skip['by_run'].update(ssm_skip.get('by_run', dict()))

    if "UPDATE_STEP" in skip['global']:
        # i.e. do not sync Workflow output from WES. Instead, just use the Workflow output in Portal DB
        this_workflow: Workflow = workflow_srv.get_workflow_by_ids(wfr_id=wfr_id, wfv_id=wfv_id)
    else:
        this_workflow = update_step(wfr_id, wfv_id, wfr_event, context)

    return next_step(this_workflow, skip, context)


def update_step(wfr_id, wfv_id, wfr_event, context):
    # eagerly sync update Workflow run output, end time, end status from WES and notify if necessary
    updated_workflow: dict = workflow_update.handler({
        'wfr_id': wfr_id,
        'wfv_id': wfv_id,
        'wfr_event': wfr_event,
    }, context)

    if updated_workflow:
        this_workflow: Workflow = workflow_srv.get_workflow_by_ids(
            wfr_id=updated_workflow['wfr_id'],
            wfv_id=updated_workflow['wfv_id']
        )
        return this_workflow

    return None


def next_step(this_workflow: Workflow, skip: dict, context=None):
    """determine next pipeline step based on this_workflow state from database

    :param skip:
    :param this_workflow:
    :param context:
    :return: None
    """
    if not this_workflow:
        logger.warning(f"Skip next step as null workflow received")
        return

    # build skip list from global list plus run specific list (if any)
    skiplist: list = skip['global']
    if this_workflow.sequence_run:
        run_id = this_workflow.sequence_run.instrument_run_id
        run_skip_list = skip['by_run'].get(run_id, [])
        skiplist.extend(run_skip_list)

    # depends on this_workflow state from db, we may kick off next workflow
    if this_workflow.type_name.lower() == WorkflowType.BCL_CONVERT.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info(f"Received successful BCL Convert workflow notification")

        # Secondary analysis stage

        WorkflowRule(this_workflow).must_associate_sequence_run().must_have_output()

        results = list()

        if "FASTQ_UPDATE_STEP" in skiplist:
            logger.info("Skip updating FASTQ entries (FastqListRows)")
        else:
            logger.info("Updating FASTQ entries (FastqListRows)")
            fastq_update_step.perform(this_workflow)

        if "GOOGLE_LIMS_UPDATE_STEP" in skiplist:
            logger.info("Skip updating Google LIMS")
        else:
            logger.info("Updating Google LIMS")
            google_lims_update_step.perform(this_workflow)

        if "DRAGEN_WGS_QC_STEP" in skiplist:
            logger.info("Skip performing DRAGEN_WGS_QC_STEP")
        else:
            logger.info("Performing DRAGEN_WGS_QC_STEP")
            results.append(dragen_wgs_qc_step.perform(this_workflow))

        if "DRAGEN_TSO_CTDNA_STEP" in skiplist:
            logger.info("Skip performing DRAGEN_TSO_CTDNA_STEP")
        else:
            logger.info("Performing DRAGEN_TSO_CTDNA_STEP")
            results.append(dragen_tso_ctdna_step.perform(this_workflow))

        if "DRAGEN_WTS_STEP" in skiplist:
            logger.info("Skip performing DRAGEN_WTS_STEP")
        else:
            logger.info("Performing DRAGEN_WTS_STEP")
            results.append(dragen_wts_step.perform(this_workflow))

        return results

    elif this_workflow.type_name.lower() == WorkflowType.DRAGEN_WTS.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info(f"Received DRAGEN_WTS workflow notification")

        WorkflowRule(this_workflow).must_associate_sequence_run().must_have_output()

        results = list()

        if "SOMALIER_EXTRACT_STEP" in skiplist:
            logger.info("Skip performing SOMALIER_EXTRACT_STEP")
        else:
            logger.info("Performing SOMALIER_EXTRACT_STEP")
            results.append(somalier_extract_step.perform(this_workflow))


        if "DRACARYS_MULTIQC_STEP" in skiplist:
            logger.info("Skip performing DRACARYS_MULTIQC_STEP")
        else:
            logger.info("Performing DRACARYS_MULTIQC_STEP")
        results.append(dracarys_multiqc_step.perform(this_workflow))

        return results

    elif this_workflow.type_name.lower() == WorkflowType.DRAGEN_WGS_QC.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info(f"Received DRAGEN_WGS_QC workflow notification")

        WorkflowRule(this_workflow).must_associate_sequence_run().must_have_output()

        results = list()

        if "SOMALIER_EXTRACT_STEP" in skiplist:
            logger.info("Skip performing SOMALIER_EXTRACT_STEP")
        else:
            logger.info("Performing SOMALIER_EXTRACT_STEP")
            results.append(somalier_extract_step.perform(this_workflow))

        if "TUMOR_NORMAL_STEP" in skiplist:
            logger.info("Skip performing TUMOR_NORMAL_STEP")
        else:
            logger.info("Performing TUMOR_NORMAL_STEP")
            results.append(tumor_normal_step.perform(this_workflow))

        return results

    # FIXME TSO500 ctDNA RUO bundle reference genome hg19
    #   https://github.com/umccr/data-portal-apis/issues/509
    #   https://github.com/umccr/cwl-ica/issues/153
    # elif this_workflow.type_name.lower() == WorkflowType.DRAGEN_TSO_CTDNA.value.lower() and \
    #         this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
    #     logger.info("Received DRAGEN_TSO_CTDNA workflow notification")
    #
    #     WorkflowRule(this_workflow).must_have_output()
    #
    #     results = list()
    #
    #     if "SOMALIER_EXTRACT_STEP" in skiplist:
    #         logger.info("Skip performing SOMALIER_EXTRACT_STEP")
    #     else:
    #         logger.info("Performing SOMALIER_EXTRACT_STEP")
    #         results.append(somalier_extract_step.perform(this_workflow))
    #
    #     return results

    elif this_workflow.type_name.lower() == WorkflowType.TUMOR_NORMAL.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info("Received TUMOR_NORMAL workflow notification")

        WorkflowRule(this_workflow).must_have_output()

        results = list()

        if "DRACARYS_MULTIQC_STEP" in skiplist:
            logger.info("Skip performing DRACARYS_MULTIQC_STEP")
        else:
            logger.info("Performing DRACARYS_MULTIQC_STEP")
        results.append(dracarys_multiqc_step.perform(this_workflow))

        if "UMCCRISE_STEP" in skiplist:
            logger.info("Skip performing UMCCRISE_STEP")
        else:
            logger.info("Performing UMCCRISE_STEP")
            results.append(umccrise_step.perform(this_workflow))

        return results

    elif this_workflow.type_name.lower() == WorkflowType.UMCCRISE.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info("Received UMCCRISE workflow notification")

        WorkflowRule(this_workflow).must_have_output()

        results = list()

        if "RNASUM_STEP" in skiplist:
            logger.info("Skip performing RNASUM_STEP")
        else:
            logger.info("Performing RNASUM_STEP")
            results.append(rnasum_step.perform(this_workflow))

        return results
        
    elif this_workflow.type_name.lower() == WorkflowType.DRAGEN_TSO_CTDNA.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info("Received DRAGEN_TSO_CTDNA workflow notification")

        WorkflowRule(this_workflow).must_have_output()

        results = list()

        if "DRACARYS_MULTIQC_STEP" in skiplist:
            logger.info("Skip performing DRACARYS_MULTIQC_STEP")
        else:
            logger.info("Performing DRACARYS_MULTIQC_STEP")
        results.append(dracarys_multiqc_step.perform(this_workflow))
