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

from libiap.openapi import libwes

from data_processors.pipeline.constant import WorkflowStatus, WorkflowRunEventType
from utils import libssm, libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_SSM_KEY_IAP_AUTH_TOKEN = "/iap/jwt-token"
DEFAULT_IAP_BASE_URL = "https://aps2.platform.illumina.com"


def configuration():
    iap_auth_token = os.getenv("IAP_AUTH_TOKEN", None)
    if iap_auth_token is None:
        iap_auth_token = libssm.get_secret(os.getenv('SSM_KEY_NAME_IAP_AUTH_TOKEN', DEFAULT_SSM_KEY_IAP_AUTH_TOKEN))
    iap_base_url = os.getenv("IAP_BASE_URL", DEFAULT_IAP_BASE_URL)

    config = libwes.Configuration(
        host=iap_base_url,
        api_key={
            'Authorization': iap_auth_token
        },
        api_key_prefix={
            'Authorization': "Bearer"
        },
    )

    # WARNING: only in local debug purpose, should never be committed uncommented!
    # it print stdout all libiap.openapi http calls activity including JWT token in http header
    # config.debug = True

    return config


def launch(event, context) -> dict:
    """event payload dict
    {
        'workflow_id': "wfl.xxx",
        'workflow_version': "v1",
        'workflow_run_name': "umccr__test__run",
        'workflow_input': {},
        'workflow_engine_parameters': {}
    }

    :param event:
    :param context:
    :return: workflow run json string
    """

    logger.info(f"Start processing WES workflow launch event")
    logger.info(libjson.dumps(event))

    # TODO: make engine parameters optional/check for presence?
    with libwes.ApiClient(configuration()) as api_client:
        version_api = libwes.WorkflowVersionsApi(api_client)
        workflow_id = event['workflow_id']
        workflow_version = event['workflow_version']
        body = libwes.LaunchWorkflowVersionRequest(
            name=event['workflow_run_name'],
            input=event['workflow_input'],
            engine_parameters=event['workflow_engine_parameters']
        )

        try:
            logger.info(f"LAUNCHING WORKFLOW_ID: {workflow_id}, VERSION_NAME: {workflow_version}, "
                        f"INPUT: \n{libjson.dumps(body.to_dict())}")
            wfl_run: libwes.WorkflowRun = version_api.launch_workflow_version(workflow_id, workflow_version, body=body)
            wfl_run_json = libjson.dumps(wfl_run.to_dict())
            logger.info(f"WORKFLOW LAUNCH SUCCESS: \n{wfl_run_json}")
            return libjson.loads(wfl_run_json)  # make datetime serialized str into format "1972-09-21T07:48:02.120Z"
        except libwes.ApiException as e:
            logger.error(f"Exception when calling launch_workflow_version: \n{e}")


def get_workflow_run(event, context) -> dict:
    """event payload dict
    {
        'wfr_id': "wfr.xxx",
        'wfr_event': {
            'event_type': "RunSucceeded",
            'event_details': {},
            'timestamp': "2020-06-24T11:27:35.1268588Z"
        }
    }

    :param event:
    :param context:
    :return: JSON contain end status, end datetime, output
    """

    logger.info(f"Start processing WES GET workflow run event")
    logger.info(libjson.dumps(event))

    wfr_id = event['wfr_id']
    wfr_event = event.get('wfr_event')

    with libwes.ApiClient(configuration()) as api_client:
        run_api = libwes.WorkflowRunsApi(api_client)

        try:
            if wfr_event:
                return _extended_get_workflow_run(wfr_id, wfr_event, run_api)

            else:
                wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id=wfr_id)
                logger.info(f"Getting '{wfr_id}' status from WES RUN API endpoint")
                _status = wfl_run.status
                _output = wfl_run.output
                _end = wfl_run.time_stopped

                result = {'status': _status, 'end': _end, 'output': _output}
                logger.info(libjson.dumps(result))
                return result

        except libwes.ApiException as e:
            logger.error(f"Exception when calling get_workflow_run: \n{e}")


def _extended_get_workflow_run(wfr_id, wfr_event, run_api):
    """
    First, it will attempt to update Workflow Run status from WES RUN endpoint.
    If WES RUN API response disagree with SQS message WES EventType then it will attempt WES RUN History endpoint.
    As last resort, it will update using SQS message WES EventType, EventDetails and Timestamp.

    See issue https://github.com/umccr-illumina/stratus/issues/114
    """

    wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id=wfr_id)

    # Running in RunStarted, a bit quirky enum nomenclature on EventType vs Status!
    # if it is this pair, it is ok to update status from WES API endpoint response
    if wfr_event and \
            wfl_run.status == WorkflowStatus.RUNNING.value and \
            wfr_event['event_type'] == WorkflowRunEventType.RUNSTARTED.value:
        logger.info(f"Getting '{wfr_id}' status from WES RUN API endpoint")
        _status = wfl_run.status
        _output = wfl_run.output
        _end = wfl_run.time_stopped

    # evaluate patterns "Failed in RunFailed", "Succeeded in RunSucceeded", "Aborted in RunAborted"
    elif wfr_event and wfl_run.status in wfr_event['event_type']:
        logger.info(f"Getting '{wfr_id}' status from WES RUN API endpoint")
        _status = wfl_run.status
        _output = wfl_run.output
        _end = wfl_run.time_stopped

    else:
        run_events = []  # collect run events
        page_token = None
        while True:
            hist: libwes.WorkflowRunHistoryEventList = run_api.list_workflow_run_history(
                run_id=wfr_id,
                page_size=1000,
                page_token=page_token
            )
            for item in hist.items:
                run_event: libwes.WorkflowRunHistoryEvent = item
                if run_event.event_type.startswith("Run"):
                    run_events.append(run_event)
            page_token = hist.next_page_token
            if not hist.next_page_token:
                break
        # end while

        # run events are sorted by event_id ASC, so grab the last event
        last_run_event: libwes.WorkflowRunHistoryEvent = run_events[-1] if run_events else None

        # evaluation patterns "RunFailed in RunFailed", "RunSucceeded in RunSucceeded", ... should match
        if last_run_event and last_run_event.event_type in wfr_event['event_type']:
            logger.info(f"Getting '{wfr_id}' status from WES RUN HISTORY API endpoint")
            _status = last_run_event.event_type[3:]  # expect first 3 char in Run*
            _end = last_run_event.timestamp  # expect in datetime.datetime
            event_details: dict = last_run_event.event_details  # expect in dict
        else:
            # last resort, log raise warning as updating directly from event message is a bit of alarming!
            logger.warning(f"Getting '{wfr_id}' status directly from SQS WES RUN EVENT message")
            _status = wfr_event['event_type'][3:]  # expect first 3 char in Run*
            _end = wfr_event['timestamp']  # expect in raw UTC datetime string
            event_details: dict = wfr_event['event_details']  # expect in dict

        if "output" in event_details:
            _output = event_details['output']
        else:
            _output = event_details

    result = {'status': _status, 'end': _end, 'output': _output}
    logger.info(libjson.dumps(result))
    return result
