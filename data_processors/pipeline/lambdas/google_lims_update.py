try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

from data_portal.models.workflow import Workflow
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
