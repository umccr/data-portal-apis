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

from data_portal.models import Workflow
from data_processors.pipeline import services
from data_processors.pipeline.lambdas import wes_handler
from data_processors.pipeline.constant import WorkflowType
from utils import libjson

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
        }
    }

    :param event:
    :param context:
    :return: workflow record from db in JSON string or None
    """

    logger.info(f"Start processing workflow update event")
    logger.info(libjson.dumps(event))

    wfr_id = event['wfr_id']
    wfv_id = event['wfv_id']
    wfr_event = event.get('wfr_event')

    wfl_in_db: Workflow = services.get_workflow_by_ids(wfr_id=wfr_id, wfv_id=wfv_id)

    if not wfl_in_db:
        logger.info(f"Run ID '{wfr_id}' is not found in Portal workflow runs automation database. Skipping...")
        return

    wes_run_resp_json = wes_handler.get_workflow_run({
        'wfr_id': wfr_id,
        'wfr_event': wfr_event,
    }, context)

    wes_run_resp: dict = libjson.loads(wes_run_resp_json)

    _status = wes_run_resp['status']
    _end = wes_run_resp['end']
    _output = wes_run_resp['output']
    _notified = wfl_in_db.notified

    # detect status has changed
    if isinstance(wfl_in_db.end_status, str):
        if wfl_in_db.end_status.lower() != _status.lower() and _notified:
            logger.info(f"{wfl_in_db.type_name} '{wfl_in_db.wfr_id}' workflow status has changed "
                        f"from '{wfl_in_db.end_status}' to '{_status}'. "
                        f"Reset notified from '{wfl_in_db.notified}' to 'False' for sending notification next.")
            _notified = False

    # update db record
    updated_workflow: Workflow = services.create_or_update_workflow(
        {
            'wfr_id': wfr_id,
            'wfv_id': wfv_id,
            'wfl_id': wfl_in_db.wfl_id,
            'type': WorkflowType[wfl_in_db.type_name.upper()],
            'end_status': _status,
            'output': _output,
            'end': _end,
            'notified': _notified,
        }
    )

    # notify workflow status
    services.notify_workflow_status(updated_workflow)

    # return some useful workflow attributes for lambda caller
    return libjson.dumps(
        {
            'id': updated_workflow.id,
            'wfr_id': updated_workflow.wfr_id,
            'wfv_id': updated_workflow.wfv_id,
            'wfl_id': updated_workflow.wfl_id,
            'end_status': updated_workflow.end_status,
            'type_name': updated_workflow.type_name,
            'sample_name': updated_workflow.sample_name,
            'seq_run_id': updated_workflow.sequence_run.run_id if updated_workflow.sequence_run else None,
            'seq_name': updated_workflow.sequence_run.name if updated_workflow.sequence_run else None,
        }
    )
