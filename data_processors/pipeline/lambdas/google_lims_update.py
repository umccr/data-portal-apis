try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---
from typing import List
from libumccr import libjson

from data_portal.models.workflow import Workflow, logger
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.orchestration import google_lims_update_step


def handler(event, context):
    """
    Update the Google LIMS sheet given either an ICA workflow run ID or an Illumina Sequence run name.
    Only one of those parameters is required.
    {
        "wfr_id": "wfr.08de0d4af8894f0e95d6bc1f58adf1dc",
        "sequence_run_name": "201027_A01052_0022_BH5KDCESXY"
    }
    :param event: request payload as above with either Workflow Run ID or Sequence Run name
    :param context: Not used
    :return: dict response of the Google Sheets update request
    """
    # we want a Workflow object, which we can get either directly using a workflow ID
    # or via a sequence run lookup/mapping
    wrf_id = event.get('wfr_id')
    seq_run_name = event.get('sequence_run_name')
    if wrf_id:
        # there should be exactly one record for any 'wfr' ID
        workflow = Workflow.objects.get(wfr_id=wrf_id)
    elif seq_run_name:
        workflow = workflow_srv.get_workflow_for_seq_run_name(seq_run_name)
    else:
        raise ValueError(f"Event does not contain expected parameters (wfr_id/sequence_run_name): {event}")

    return google_lims_update_step.perform(workflow)


def by_provided_id(event, context):
    """
    Update Google LIMS by provided ID. Event payload as follows.

    {
        "instrument_run_id": "201027_A01052_0022_BH5KDCESXY",
        "libraries": [
            "L0000001",
            "L0000002",
            "L0000003"
        ]
    }
    """

    logger.info(libjson.dumps(event))

    instrument_run_id = event['instrument_run_id']
    libraries = event['libraries']

    if not isinstance(instrument_run_id, str):
        return {"Error": "The payload instrument_run_id must be string"}

    if not isinstance(libraries, List):
        return {"Error": "The payload libraries must be list of LibraryID string"}

    return google_lims_update_step.perform_by_provided_id(libraries=libraries, instrument_run_id=instrument_run_id)
