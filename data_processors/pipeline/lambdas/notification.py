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

from data_processors.pipeline.services import batch_srv, notification_srv
from libumccr import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def sqs_handler(event, context):
    """event payload dict
    {
        'Records': [
            {
                'messageId': "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                'body': "{\"JSON\": \"Formatted Message\"}",
                'messageAttributes': {},
                'md5OfBody': "",
                'eventSource': "aws:sqs",
                'eventSourceARN': "arn:aws:sqs:us-east-2:123456789012:fifo.fifo",
            },
            ...
        ]
    }

    Details event payload dict refer to https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    Backing queue is FIFO queue and, guaranteed delivery-once, no duplication.

    :param event:
    :param context:
    :return:
    """
    messages = event['Records']

    results = []
    batch_item_failures = []
    for message in messages:
        job = libjson.loads(message['body'])
        try:
            results.append(handler(job, context))
        except Exception as e:
            logger.exception(str(e), exc_info=e, stack_info=True)

            # SQS Implement partial batch responses - ReportBatchItemFailures
            # https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
            # https://repost.aws/knowledge-center/lambda-sqs-report-batch-item-failures
            batch_item_failures.append({
                "itemIdentifier": message['messageId']
            })

    return {
        'results': results,
        'batchItemFailures': batch_item_failures
    }


def handler(event, context):
    """event payload dict
    {
        'batch_run_id': 1
    }

    :param event:
    :param context:
    :return:
    """
    batch_run_id = event['batch_run_id']

    batch_run = batch_srv.get_batch_run_none_or_all_running(batch_run_id=batch_run_id)
    if batch_run:
        logger.info(f"[RUNNING] Batch Run ID [{batch_run.id}]. Processing notification.")
        resp = notification_srv.notify_batch_run_status(batch_run_id=batch_run.id)
        return {
            'batch_run_id': batch_run_id,
            'notification': resp,
        }

    if batch_run is None:
        batch_run = batch_srv.get_batch_run_none_or_all_completed(batch_run_id=batch_run_id)
        if batch_run:
            logger.info(f"[COMPLETED] Batch Run ID [{batch_run.id}]. Processing notification.")
            resp = notification_srv.notify_batch_run_status(batch_run_id=batch_run.id)
            return {
                'batch_run_id': batch_run_id,
                'notification': resp,
            }

    # otherwise skip to wait until the last workflow in the batch has arrived
    logger.info(f"[SKIP] Batch Run ID [{batch_run_id}] notification. Waiting other samples in batch run.")
    return {
        'batch_run_id': batch_run_id,
        'notification': "SKIP",
    }
