import logging
from ast import literal_eval
from collections import defaultdict
from enum import Enum
from typing import List, Tuple, Dict

from dateutil.parser import parse

from data_processors import const
from data_processors.s3 import services

logger = logging.getLogger(__name__)
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


def is_report_record(s3_object_meta):
    """
    Check S3 object key to determine whether further processing is required for report data ingestion
    Filtering strategy is finding a very discriminated "keyword" in S3 object key
    """
    key = s3_object_meta['key']
    return const.JSON_GZ in key and const.CANCER_REPORT_TABLES in key


def parse_raw_s3_event_records(messages: List[dict]) -> Dict:
    """
    Parse raw SQS messages into S3EventRecord objects
    :param messages: the messages to be processed
    :return: list of S3EventRecord objects
    """
    s3_event_records = []
    report_event_records = []

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

            logger.debug(f"Found new event of type {event_type}")

            s3_event_records.append(S3EventRecord(event_type, event_time, s3_bucket_name, s3_object_meta))

            # filter early for records that need further processing
            if is_report_record(s3_object_meta):
                report_event_records.append(S3EventRecord(event_type, event_time, s3_bucket_name, s3_object_meta))

    return {
        's3_event_records': s3_event_records,
        'report_event_records': report_event_records
    }


def sync_s3_event_records(records: List[S3EventRecord]) -> dict:
    """
    Synchronise s3 event records to the db.
    :param records: records to be processed
    :return results of synchronisation
    """
    results = defaultdict(int)

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
            logger.info(f"Found unsupported S3 event type: {record.event_type}")
            results['unsupported_count'] += 1

    return results


def sync_s3_event_record_removed(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (REMOVED) record to db
    :param record: record to be synced
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    return services.delete_s3_object(bucket_name, key)


def sync_s3_event_record_created(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (CREATED) record to db
    :return: number of s3 object created, number of database association records created
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    size = record.s3_object_meta['size']
    e_tag = record.s3_object_meta['eTag']

    services.tag_s3_object(bucket_name, key, "bam")

    return services.persist_s3_object(
        bucket=bucket_name, key=key, size=size, last_modified_date=record.event_time, e_tag=e_tag
    )
