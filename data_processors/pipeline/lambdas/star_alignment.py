try:
    import unzip_requirements
except ImportError:
    pass

import os
import django
from libumccr import libjson

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def sqs_handler(event, context):
    """
    Unpack body from SQS wrapper.
    SQS event payload dict:
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
    for message in messages:
        job = libjson.loads(message['body'])
        results.append(handler(job, context))

    return {
        'results': results
    }


def handler(event, context) -> dict:
    """event payload dict
    {
        ...
    }
    """

    # todo WorkflowHelper()
    #  create new Workflow entry to database
    #  establish link between Workflow and LibraryRun
    #  parse job JSON from event for upstream Lambda payload
    #  call upstream Lambda with Event invocation
    #  ref impl -->  data_processors.pipeline.lambdas.dragen_wts.handler

    pass
