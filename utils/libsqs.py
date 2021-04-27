# -*- coding: utf-8 -*-
"""libsqs module

Module interface for dealing with SQS queue and plus some useful dispatching batch jobs impl

https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
"""
import logging
import uuid
from typing import List

from utils import libjson, libaws

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 10


def _build(queue_arn):
    client = libaws.sqs_client()
    queue_url = client.get_queue_url(QueueName=arn_to_name(queue_arn))['QueueUrl']
    return client, queue_url


def arn_to_name(arn: str):
    """Get queue name from given SQS ARN"""
    arr = arn.split(':')
    if isinstance(arr, list):
        return arr[-1]
    return None


def dispatch_notification(queue_arn: str, message: dict, group_id: str):
    """
    Note: backing notification queue is FIFO Delay queue with ContentBasedDeduplication enabled.
    Hence, if sha256(message) is the same, it will get dedup.
    If group_id is set to the same value, message will be enqueue in FIFO order.

    The main use case here is to serialize simultaneous distributed event messages into a batch notification aggregate.

    :param queue_arn:
    :param message:
    :param group_id:
    :return:
    """

    response = enqueue_message(
        queue_arn=queue_arn,
        MessageBody=libjson.dumps(message),
        MessageGroupId=group_id,
    )

    logger.info(f"NOTIFICATION QUEUE RESPONSE: \n{libjson.dumps(response)}")
    return response


def dispatch_jobs(queue_arn, job_list, batch_size=10, fifo=True):
    """
    Queue job in batch of given size

    See [1] for message format which to be consumed by SQS Lambda trigger

    If it is on SQS Lambda trigger consumer, less batch_size mean more parallel lambda invocations i.e. it goes
    as low as 1 message per lambda invocation. Messages enqueue within the same group_id are guaranteed FIFO order.
    Also guaranteed delivery-once and deduplicate based on hash(message body content).

    Example use case for how to adjust batch_size to Lambda concurrency:
    Say, calling WES endpoint for workflow launch take approx. 1s
    Then, 10 messages per batch for launching 10 workflows = 1s * 10 (+ headroom for warmup, complexity) = say est. 20s

    Default Lambda execution timeout is 6s -- configurable upto 900s.
    However typical Lambda prefer timeout setting is 30s, or less.
    Also note that SQS the default visibility timeout for a message is 30 seconds [2].
    [1] say to set the source queue's visibility timeout to at least 6 times the timeout that of Lambda function.

    Hence observe and do Maths around this for optimal operational setting.

    [1]: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    [2]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html

    :param queue_arn:
    :param job_list:
    :param batch_size:
    :param fifo: default True
    :return:
    """
    _batch_size = batch_size if batch_size < MAX_BATCH_SIZE else MAX_BATCH_SIZE
    _chunks = [job_list[x:x + _batch_size] for x in range(0, len(job_list), _batch_size)]
    responses = {}
    for chunk in _chunks:
        entries = []
        group_id = str(uuid.uuid4())
        for job in chunk:
            entry = {
                'Id': str(uuid.uuid4()),
                'MessageBody': libjson.dumps(job),
            }
            if fifo:
                entry['MessageGroupId'] = group_id
            entries.append(entry)
        resp = enqueue_messages(queue_arn, entries)
        responses[group_id] = {k: v for k, v in resp.items() if k.startswith('Successful') or k.startswith('Failed')}

    logger.info(f"JOB QUEUE RESPONSE: \n{libjson.dumps(responses)}")
    return responses


def enqueue_messages(queue_arn: str, entries: List[dict]):
    """
    Enqueue batch message entries into given queue

    :param queue_arn:
    :param entries:
    :return:
    """
    client, queue_url = _build(queue_arn=queue_arn)
    return client.send_message_batch(QueueUrl=queue_url, Entries=entries)


def enqueue_message(queue_arn: str, **kwargs):
    """
    Enqueue a message

    :param queue_arn:
    :param kwargs:
    :return:
    """
    client, queue_url = _build(queue_arn=queue_arn)
    return client.send_message(QueueUrl=queue_url, **kwargs)
