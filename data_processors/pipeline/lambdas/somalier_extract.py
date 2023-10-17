# -*- coding: utf-8 -*-
try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from libumccr import libjson

from data_processors.pipeline.domain.somalier import HolmesPipeline, HolmesExtractDto, SomalierReferenceSite

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


def handler(event, context) -> dict:
    """event payload dict
    {
        "index": "gds://volume/path/to/this.bam",       (MANDATORY)
        "reference": "hg38.rna"  (OR)  "hg19.rna"       (MANDATORY)
    }

    :param event:
    :param context:
    :return: dict response of the aws step function launched
    """

    logger.info(f"Start processing somalier extract")
    logger.info(libjson.dumps(event))

    index: str = event['index']
    reference_str: str = event['reference']

    dto = HolmesExtractDto(
        run_name=HolmesPipeline.get_step_function_instance_name(prefix="somalier_extract", index=index),
        indexes=[index],
        reference=SomalierReferenceSite.from_value(reference_str),
    )

    # at the mo, it is 'fire & forget' for fingerprint extraction
    holmes_pipeline = (
        HolmesPipeline()
        .extract(dto)
    )

    logger.info(f"Extracting fingerprint from '{index}' with "
                f"step function instance of '{holmes_pipeline.execution_arn}'")

    # Return execution instance info as-is
    return libjson.loads(libjson.dumps(holmes_pipeline.execution_instance))
