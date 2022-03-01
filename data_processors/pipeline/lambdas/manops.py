try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from libumccr.aws import libsqs, libssm

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_RNASUM_QUEUE_ARN
from data_processors.pipeline.orchestration import rnasum_step
from data_processors.pipeline.services import workflow_srv

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _halt(error_msg, event):
    logger.warning(error_msg)
    return {
        "error": error_msg,
        "event": event
    }


def handler(event, context) -> dict:
    """event router payload dict
    {
        "event_type": "rnasum",
        ...
        (forwarding event)
    }
    """

    event_type = event['event_type']

    if str(event_type).lower() == "RNASUM".lower():
        return rnasum_handler(event, context)
    else:
        return _halt("NOT_SUPPORTED_EVENT", event)


def rnasum_handler(event, context) -> dict:
    """event payload dict
    {
        "wfr_id": "wfr.xx",
        "wfv_id": "wfv.xx",
        "dataset": "BRCA"
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """
    wfr_id = event['wfr_id']
    wfv_id = event.get('wfv_id', None)
    dataset = event['dataset']

    umccrise_workflow: Workflow = workflow_srv.get_workflow_by_ids(wfr_id=wfr_id, wfv_id=wfv_id)

    if not umccrise_workflow:
        return _halt("Can not find umccrise workflow run event in Portal", event)

    if not dataset:
        return _halt("Dataset must not be null", event)

    job_list = rnasum_step.prepare_rnasum_jobs(umccrise_workflow)

    if job_list:
        for job in job_list:
            job['dataset'] = dataset  # override dataset

        # now dispatch to rnasum job queue
        _ = libsqs.dispatch_jobs(queue_arn=libssm.get_ssm_param(SQS_RNASUM_QUEUE_ARN), job_list=job_list)
        msg = "Succeeded"

    else:
        msg = f"Calling to prepare_rnasum_jobs() return empty list, no job to dispatch..."
        logger.warning(msg)

    return {
        "message": msg,
        "job_list": job_list,
        "event": event
    }
