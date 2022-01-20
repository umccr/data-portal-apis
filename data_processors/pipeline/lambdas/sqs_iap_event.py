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
import logging

from data_portal.models.sequence import SequenceStatus
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.libraryrun import LibraryRun
from data_processors.pipeline.domain.workflow import SequenceRule, SequenceRuleError
from data_processors.pipeline.services import notification_srv, sequence_srv, sequencerun_srv, libraryrun_srv
from data_processors.pipeline.lambdas import bcl_convert, orchestrator
from libumccr import libjson
from libica.app import ENSEventType

logger = logging.getLogger()
logger.setLevel(logging.INFO)

IMPLEMENTED_ENS_TYPES = [
    ENSEventType.BSSH_RUNS.value,
    ENSEventType.WES_RUNS.value,
]


def handler(event, context):
    """event payload dict
    SQS WES Event message format refer to test module in test_sqs_iap_event

    :param event:
    :param context:
    :return:
    """
    logger.info("Start processing IAP ENS event")
    logger.info(libjson.dumps(event))

    messages = event['Records']

    for message in messages:
        event_type = message['messageAttributes']['type']['stringValue']

        if event_type not in IMPLEMENTED_ENS_TYPES:
            logger.warning(f"Skipping unsupported IAP ENS type: {event_type}")
            continue

        event_action = message['messageAttributes']['action']['stringValue']
        message_body = libjson.loads(message['body'])

        if event_type == ENSEventType.BSSH_RUNS.value:
            handle_bssh_run_event(message, event_action, event_type, context)

        if event_type == ENSEventType.WES_RUNS.value:
            handle_wes_runs_event(message_body, context)

    _msg = f"IAP ENS event processing complete"
    logger.info(_msg)
    return _msg


def handle_bssh_run_event(message, event_action, event_type, context):
    payload = {}
    payload.update(libjson.loads(message['body']))
    payload.update(messageAttributesAction=event_action)
    payload.update(messageAttributesActionType=event_type)
    payload.update(messageAttributesActionDate=message['messageAttributes']['actiondate']['stringValue'])
    payload.update(messageAttributesProducedBy=message['messageAttributes']['producedby']['stringValue'])
    sqr: SequenceRun = sequencerun_srv.create_or_update_sequence_run(payload)
    if sqr:
        ts = message['attributes']['ApproximateFirstReceiveTimestamp']
        aws_account = message['eventSourceARN'].split(':')[4]
        notification_srv.notify_sequence_run_status(sqr=sqr, sqs_record_timestamp=int(ts), aws_account=aws_account)

        # Create or update Sequence record from BSSH Run event payload
        this_sequence = sequence_srv.create_or_update_sequence_from_bssh_event(payload)

        # Populate LibraryRun records
        if this_sequence.status.lower() == SequenceStatus.STARTED.value.lower() \
                or sqr.status.lower() == "PendingAnalysis".lower():
            library_run_list: List[LibraryRun] = libraryrun_srv.create_library_run_from_sequence({
                'instrument_run_id': this_sequence.instrument_run_id,
                'run_id': this_sequence.run_id,
                'gds_folder_path': this_sequence.gds_folder_path,
                'gds_volume_name': this_sequence.gds_volume_name,
                'sample_sheet_name': this_sequence.sample_sheet_name,
            })

        # Skip any further processing if a Run is registered in the emergency stop list
        try:
            SequenceRule(this_sequence).must_not_emergency_stop()
        except SequenceRuleError as se:
            reason = f"Aborted ICA pipeline. {se}"
            logger.warning(reason)
            event = {
                'gds_volume_name': this_sequence.gds_volume_name,
                'gds_folder_path': this_sequence.gds_folder_path,
                'seq_run_id': this_sequence.run_id,
                'seq_name': this_sequence.instrument_run_id,
            }
            notification_srv.notify_outlier(topic="Due to Emergency Stop", reason=reason, status="Aborted", event=event)
            return

        # Using bssh.runs event status PendingAnalysis as trigger for next event
        # See https://github.com/umccr-illumina/stratus/issues/95
        if sqr.status.lower() == "PendingAnalysis".lower():
            # launch bcl convert workflow
            bcl_convert.handler({
                'gds_volume_name': sqr.gds_volume_name,
                'gds_folder_path': sqr.gds_folder_path,
                'seq_run_id': sqr.run_id,
                'seq_name': sqr.name,
            }, context)


def handle_wes_runs_event(message_body, context):
    wfr_event = {
        'event_type': message_body['EventType'],
        'event_details': message_body['EventDetails'],
        'timestamp': message_body['Timestamp'],
    }
    wfr_id = message_body['WorkflowRun']['Id']
    wfv_id = message_body['WorkflowRun']['WorkflowVersion']['Id']

    orchestrator.handler({
        'wfr_id': wfr_id,
        'wfv_id': wfv_id,
        'wfr_event': wfr_event,
    }, context)
