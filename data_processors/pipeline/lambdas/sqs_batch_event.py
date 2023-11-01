try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import json
import logging
from enum import Enum
from types import SimpleNamespace
from datetime import datetime

from libumccr import libjson
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.domain.event.wrsc import WorkflowRunStateChangeEnvelope, WorkflowRunStateChange

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class EventSource(Enum):
    AWS_BATCH = "aws.batch"
    AWS_SQS = "aws:sqs"

    @classmethod
    def from_value(cls, value):
        if value == cls.AWS_BATCH.value:
            return cls.AWS_BATCH
        elif value == cls.AWS_SQS.value:
            return cls.AWS_SQS
        else:
            raise ValueError(f"No matching type found for {value}")


def handler(event, context):
    """event payload dict
    Native AWS Batch event envelope wrapped within SQS event structure

    See https://umccr.slack.com/archives/CP356DDCH/p1693195057311949
    """
    logger.info("Start processing AWS Batch event")
    logger.info(libjson.dumps(event))

    messages = event['Records']

    results = []
    for message in messages:

        # parse outer SQS event
        event_source = message['eventSource']
        if event_source != EventSource.AWS_SQS.value:
            logger.warning(f"Skipping unsupported event source: {event_source}")
            continue

        # parse inner Batch event
        batch_event = json.loads(message['body'], object_hook=lambda d: SimpleNamespace(**d))
        if batch_event.source == EventSource.AWS_BATCH.value:
            handle_aws_batch_event(batch_event, context)

        if batch_event.source != EventSource.AWS_BATCH.value:
            logger.warning(f"Skipping unsupported inner event source: {batch_event.source}")
            continue

        results.append(message['messageId'])

    return {
        'results': results,
    }


def handle_aws_batch_event(batch_event: SimpleNamespace, context):
    """ImplNote:
    Mapping
        Nextflow pipeline stack hasn't really fronted by any WES.
        They are just plain Batch event with AWS envelope.
        Here, we need to map this Batch event structure to harmonise with how we store
        ICA WES workflow in Portal Workflow table.
    Alternatively,
        We should have stored / created a specific entity model (separate table) for capturing this
        oncoanalyser workflow struct.
    But,
        These considerations are all pushed aside.
    Portal will just work with what option avail-at-hand here as first cut MVP. Or, this is the way, perhaps!
    See discussion https://umccr.slack.com/archives/C05784MU2F5/p1691973270555429
    One for future interation and/or OrcaBus...
    ~victor
    """

    # See Batch API
    # https://docs.aws.amazon.com/batch/latest/APIReference/API_JobDetail.html
    # https://docs.aws.amazon.com/batch/latest/APIReference/API_ContainerDetail.html

    # Map external event into internal "domain event" representation
    # See the contract https://umccr.slack.com/archives/CP356DDCH/p1694066750376839

    portal_run_id = batch_event.detail.parameters.portal_run_id

    # handle `end` time from Batch event outer attribute `stoppedAt` which is unix utc timestamp in milliseconds
    end = None
    if hasattr(batch_event.detail, "stoppedAt"):
        end = datetime.utcfromtimestamp(int(batch_event.detail.stoppedAt) / 1000)

    wrsc_envelope = WorkflowRunStateChangeEnvelope(
        id=batch_event.id,
        source=batch_event.source,
        time=batch_event.time,
        detail=WorkflowRunStateChange(
            portal_run_id=portal_run_id,
            type_name=batch_event.detail.parameters.workflow,
            version=batch_event.detail.parameters.version,
            output=batch_event.detail.parameters.output,
            wfr_name=batch_event.detail.jobName,
            wfr_id=batch_event.detail.jobId,
            wfv_id=batch_event.detail.jobDefinition,
            wfl_id=batch_event.detail.container.image,
            end_status=str(batch_event.detail.status).title(),  # makes SUCCEEDED to Succeeded
            end=end
        )
    )

    orchestrator.handler_ng({
        'portal_run_id': portal_run_id,
        'wfr_event': wrsc_envelope.model_dump(by_alias=True)  # convert to dict
    }, context)
