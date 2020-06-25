try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import json
import logging

from data_processors import services as srv
from data_processors.exceptions import *
from data_processors.pipeline.dto import WorkflowType, FastQReadType
from data_processors.pipeline.workflow import WorkflowDomainModel, WorkflowSpecification

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GDS_FILES = 'gds.files'
BSSH_RUNS = 'bssh.runs'
WES_RUNS = 'wes.runs'
IMPLEMENTED_ENS_TYPES = [GDS_FILES, BSSH_RUNS, WES_RUNS]


def handler(event, context):
    logger.info("Start processing IAP ENS event")
    logger.info(event)

    messages = event['Records']

    for message in messages:
        event_type = message['messageAttributes']['type']['stringValue']

        if event_type not in IMPLEMENTED_ENS_TYPES:
            raise UnsupportedIAPEventNotificationServiceType(event_type)

        event_action = message['messageAttributes']['action']['stringValue']
        message_body_json = json.loads(message['body'])

        if event_type == GDS_FILES:
            if event_action == 'deleted':
                srv.delete_gds_file(message_body_json)
            else:
                srv.create_or_update_gds_file(message_body_json)

        if event_type == BSSH_RUNS:
            payload = {}
            payload.update(message_body_json)
            payload.update(messageAttributesAction=event_action)
            payload.update(messageAttributesActionType=event_type)
            payload.update(messageAttributesActionDate=message['messageAttributes']['actiondate']['stringValue'])
            payload.update(messageAttributesProducedBy=message['messageAttributes']['producedby']['stringValue'])
            sqr = srv.create_or_update_sequence_run(payload)
            if sqr:
                ts = message['attributes']['ApproximateFirstReceiveTimestamp']
                aws_account = message['eventSourceARN'].split(':')[4]
                srv.send_slack_message(sqr=sqr, sqs_record_timestamp=int(ts), aws_account=aws_account)

                # Once Sequence Run status is good, launch bcl convert workflow
                if sqr.status.lower() == "PendingAnalysis".lower() or sqr.status.lower() == "Complete".lower():
                    spec = WorkflowSpecification()
                    spec.sequence_run = sqr
                    spec.workflow_type = WorkflowType.BCL_CONVERT
                    # TODO check whether we can predetermine fastq read type for bcl convert?
                    # spec.fastq_read_type = FastQReadType.PAIRED_END
                    WorkflowDomainModel(spec).launch()

        if event_type == WES_RUNS:
            # update workflow run output, end time and end status
            # extract wfr_id from message and query Workflow from db
            wfr_id = message_body_json['WorkflowRun']['Id']
            wfv_id = message_body_json['WorkflowRun']['WorkflowVersion']['Id']
            status: str = message_body_json['WorkflowRun']['Status']
            workflow = srv.get_workflow_by_ids(wfr_id=wfr_id, wfv_id=wfv_id)

            if workflow:
                # if workflow is in Portal db then WorkflowDomainModel(spec).update(workflow)
                spec = WorkflowSpecification()
                spec.workflow_type = WorkflowType[workflow.type_name]
                if workflow.fastq_read_type_name is not None:
                    spec.fastq_read_type = FastQReadType[workflow.fastq_read_type_name]
                spec.sequence_run = workflow.sequence_run
                this_model: WorkflowDomainModel = WorkflowDomainModel(spec)
                this_model.update(workflow)

                # then depends on this_model workflow type and/or other pipeline decision factor/logic
                # we may kick off next_model workflow i.e. WorkflowDomainModel(next_spec).launch()
                if this_model.workflow_type == WorkflowType.BCL_CONVERT and "Succeeded".lower() == status.lower():
                    germline_spec = WorkflowSpecification()
                    germline_spec.workflow_type = WorkflowType.GERMLINE
                    germline_spec.parents = [workflow]
                    # optionally propagate these attributes
                    germline_spec.sequence_run = workflow.sequence_run
                    if workflow.fastq_read_type_name is not None:
                        germline_spec.fastq_read_type = FastQReadType[workflow.fastq_read_type_name]
                    next_model: WorkflowDomainModel = WorkflowDomainModel(germline_spec)
                    next_model.launch()
            else:
                logger.info(f"Run ID '{wfr_id}' is not found in Portal workflow runs automation database. Skipping...")

    logger.info("IAP ENS event processing complete")
