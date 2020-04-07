try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# ---

import logging
from ast import literal_eval
from enum import Enum
from typing import List, Tuple, Union, Dict
from dateutil.parser import parse
from django.db import transaction
import traceback
from collections import defaultdict

from data_processors.services import persist_s3_object, delete_s3_object, tag_s3_object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3EventType(Enum):
    """
    See S3 Supported Event Types
    https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#supported-notification-event-types
    """
    EVENT_OBJECT_CREATED = 'ObjectCreated'
    EVENT_OBJECT_REMOVED = 'ObjectRemoved'
    EVENT_UNSUPPORTED = 'Unsupported'


class S3EventRecord:
    """
    A helper class for S3 event data passing and retrieval
    """

    def __init__(self, event_type, event_time, s3_bucket_name, s3_object_meta) -> None:
        self.event_type = event_type
        self.event_time = event_time
        self.s3_bucket_name = s3_bucket_name
        self.s3_object_meta = s3_object_meta


def handler(event: dict, context) -> Union[bool, Dict[str, int]]:
    """
    Entry point for SQS event processing
    :param event: SQS event
    """
    logger.info("Start processing")
    logger.info(event)

    messages = event['Records']

    try:
        records = parse_raw_s3_event_records(messages)
        return sync_s3_event_records(records)
    except Exception as e:
        logger.error("An unexpected error occurred!" + traceback.format_exc())
        return False


def parse_raw_s3_event_records(messages: List[dict]) -> List[S3EventRecord]:
    """
    Parse raw SQS messages into S3EventRecord objects
    :param messages: the messages to be processed
    :return: list of S3EventRecord objects
    """
    s3_event_records = []

    for message in messages:
        # We need to convert json data in string to dict
        records = literal_eval(message['body'])['Records']

        for record in records:
            event_name = record['eventName']
            event_time = parse(record['eventTime'])
            s3 = record['s3']
            s3_bucket_name = s3['bucket']['name']
            s3_object_meta = s3['object']

            # Check event type
            if S3EventType.EVENT_OBJECT_CREATED.value in event_name:
                event_type = S3EventType.EVENT_OBJECT_CREATED
            elif S3EventType.EVENT_OBJECT_REMOVED.value in event_name:
                event_type = S3EventType.EVENT_OBJECT_REMOVED
            else:
                event_type = S3EventType.EVENT_UNSUPPORTED

            logger.info("Found new event of type %s" % event_type)

            s3_event_records.append(S3EventRecord(
                event_type, event_time, s3_bucket_name, s3_object_meta
            ))

    return s3_event_records


def sync_s3_event_records(records: List[S3EventRecord]) -> dict:
    """
    Synchronise s3 event records to the db.
    :param records: records to be processed
    :return results of synchronisation
    """
    results = defaultdict(int)

    with transaction.atomic():
        for record in records:
            if record.event_type == S3EventType.EVENT_OBJECT_REMOVED:
                removed_count, s3_lims_removed_count = sync_s3_event_record_removed(record)
                results['removed_count'] += removed_count
                results['s3_lims_removed_count'] += s3_lims_removed_count

            elif record.event_type == S3EventType.EVENT_OBJECT_CREATED:
                created_count, s3_lims_created_count = sync_s3_event_record_created(record)
                results['created_count'] += created_count
                results['s3_lims_created_count'] += s3_lims_created_count
            else:
                logger.error("Found unsupported S3 event type: %s" % record.event_type)
                results['unsupported_count'] += 1

    logger.info("Synchronisation complete")
    return results


def sync_s3_event_record_removed(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (REMOVED) record to db
    :param record: record to be synced
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    bucket_name = record.s3_bucket_name
    # Removing the matched S3Object
    key = record.s3_object_meta['key']
    logger.info("Deleting an existing S3Object (bucket=%s, key=%s)" % (bucket_name, key))

    return delete_s3_object(bucket_name, key)


def sync_s3_event_record_created(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (CREATED) record to db
    :return: number of s3 object created, number of s3-lims association records created
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    size = record.s3_object_meta['size']
    e_tag = record.s3_object_meta['eTag']

    tag_s3_object(bucket_name, key)

    return persist_s3_object(
        bucket=bucket_name, key=key, size=size, last_modified_date=record.event_time, e_tag=e_tag
    )
