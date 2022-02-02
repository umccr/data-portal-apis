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
from typing import List

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.orchestration import dragen_wgs_qc_step, tumor_normal_step, google_lims_update_step, \
    dragen_tso_ctdna_step, fastq_update_step, dragen_wts_step, umccrise_step, rnasum_step
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
        'skip': [
            "UPDATE_STEP",
            "FASTQ_UPDATE_STEP",
            "GOOGLE_LIMS_UPDATE_STEP",
            "DRAGEN_WGS_QC_STEP",
            "DRAGEN_TSO_CTDNA_STEP",
            "TUMOR_NORMAL_STEP",
            "DRAGEN_WTS_STEP",
            "UMCCRISE_STEP",
            "RNASUM_STEP"
        ]
    }

    NOTE:
    Orchestrator will read static configuration step_skip_list from SSM param. And merge with skip list from the event.
    For example, to skip DRAGEN_WGS_QC_STEP, simply payload value like as follows. To reset, just payload empty list [].

        aws ssm put-parameter \
          --name "/iap/workflow/step_skip_list" \
          --type "String" \
          --value "[\"DRAGEN_WGS_QC_STEP\"]" \
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
    skip = event.get('skip', list())    # skip is optional

    try:
        step_skip_list_json = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/step_skip_list")
        step_skip_list = libjson.loads(step_skip_list_json)
    except Exception as e:
        # If any exception found, log warning and proceed
        logger.warning(f"Cannot read step_skip_list from SSM param. Exception: {e}")
        step_skip_list = []
    skip = skip + step_skip_list

    if "UPDATE_STEP" in skip:
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


def next_step(this_workflow: Workflow, skip: List[str], context=None):
    """determine next pipeline step based on this_workflow state from database

    :param skip:
    :param this_workflow:
    :param context:
    :return: None
    """
    if not this_workflow:
        logger.warning(f"Skip next step as null workflow received")
        return

    # depends on this_workflow state from db, we may kick off next workflow
    if this_workflow.type_name.lower() == WorkflowType.BCL_CONVERT.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info(f"Received successful BCL Convert workflow notification")

        # Secondary analysis stage

        WorkflowRule(this_workflow).must_associate_sequence_run().must_have_output()

        results = list()

        if "FASTQ_UPDATE_STEP" in skip:
            logger.info("Skip updating FASTQ entries (FastqListRows)")
        else:
            logger.info("Updating FASTQ entries (FastqListRows)")
            fastq_update_step.perform(this_workflow)

        if "GOOGLE_LIMS_UPDATE_STEP" in skip:
            logger.info("Skip updating Google LIMS")
        else:
            logger.info("Updating Google LIMS")
            google_lims_update_step.perform(this_workflow)

        if "DRAGEN_WGS_QC_STEP" in skip:
            logger.info("Skip performing DRAGEN_WGS_QC_STEP")
        else:
            logger.info("Performing DRAGEN_WGS_QC_STEP")
            results.append(dragen_wgs_qc_step.perform(this_workflow))

        if "DRAGEN_TSO_CTDNA_STEP" in skip:
            logger.info("Skip performing DRAGEN_TSO_CTDNA_STEP")
        else:
            logger.info("Performing DRAGEN_TSO_CTDNA_STEP")
            results.append(dragen_tso_ctdna_step.perform(this_workflow))

        if "DRAGEN_WTS_STEP" in skip:
            logger.info("Skip performing DRAGEN_WTS_STEP")
        else:
            logger.info("Performing DRAGEN_WTS_STEP")
            results.append(dragen_wts_step.perform(this_workflow))

        return results

    elif this_workflow.type_name.lower() == WorkflowType.DRAGEN_WGS_QC.value.lower():
        logger.info(f"Received DRAGEN_WGS_QC workflow notification")

        WorkflowRule(this_workflow).must_associate_sequence_run()

        results = list()

        if "TUMOR_NORMAL_STEP" in skip:
            logger.info("Skip performing TUMOR_NORMAL_STEP")
        else:
            logger.info("Performing TUMOR_NORMAL_STEP")
            results.append(tumor_normal_step.perform(this_workflow))

        return results

    elif this_workflow.type_name.lower() == WorkflowType.TUMOR_NORMAL.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info("Received TUMOR_NORMAL workflow notification")

        WorkflowRule(this_workflow).must_have_output()

        results = list()

        if "UMCCRISE_STEP" in skip:
            logger.info("Skip performing UMCCRISE_STEP")
        else:
            logger.info("Performing UMCCRISE_STEP")
            results.append(umccrise_step.perform(this_workflow))

        return results

    elif this_workflow.type_name.lower() == WorkflowType.UMCCRISE.value.lower() and \
            this_workflow.end_status.lower() == WorkflowStatus.SUCCEEDED.value.lower():
        logger.info("Received UMCCRISE workflow notification")
        # TODO: also check for WTS workflow notification?
        WorkflowRule(this_workflow).must_have_output()

        results = list()

        if "RNASUM_STEP" in skip:
            logger.info("Skip performing RNASUM_STEP")
        else:
            logger.info("Performing RNASUM_STEP")
            results.append(rnasum_step.perform(this_workflow))

        return results
