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


def dispatch_jobs(queue_name, job_list, batch_size=10):
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

    :param queue_name:
    :param job_list:
    :param batch_size:
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
                'MessageGroupId': group_id,
            }
            entries.append(entry)
        resp = queue_messages(queue_name, entries)
        responses[group_id] = {k: v for k, v in resp.items() if k.startswith('Successful') or k.startswith('Failed')}

    logger.info(f"JOB QUEUE RESPONSE: \n{libjson.dumps(responses)}")
    return responses


def queue_messages(queue_name: str, entries: List[dict]):
    """
    Queue message entries to given queue name

    :param queue_name:
    :param entries:
    :return:
    """
    client = libaws.sqs_client()
    queue_url = client.get_queue_url(QueueName=queue_name)['QueueUrl']
    return client.send_message_batch(QueueUrl=queue_url, Entries=entries)
