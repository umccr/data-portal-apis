import os, django
from ast import literal_eval

import boto3

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

from enum import Enum
from typing import List
from dateutil.parser import parse
import logging
from django.db import transaction
from django.db.models import Q

import migrate
from data_portal.models import Configuration, S3Object, LIMSRow, S3LIMS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Migrating")
migrate.main()


class EventType(Enum):
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


def handler(event: dict, context):
    """
    Entry point for SQS event processing
    :param event: SQS event
    """
    logger.info("Start processing")
    logger.info(event)

    messages = event['Records']

    try:
        check_and_sync_lims_rows()
        records = parse_raw_s3_event_records(messages)
        sync_s3_event_records(records)
    except Exception as e:
        logger.error("An unexpected error occurred: " + str(e))
        return False

    logger.info("Complete")
    return True


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
            if EventType.EVENT_OBJECT_CREATED.value in event_name:
                event_type = EventType.EVENT_OBJECT_CREATED
            elif EventType.EVENT_OBJECT_REMOVED.value in event_name:
                event_type = EventType.EVENT_OBJECT_REMOVED
            else:
                event_type = EventType.EVENT_UNSUPPORTED

            logger.info("Found new event of type %s" % event_type)

            s3_event_records.append(S3EventRecord(
                event_type, event_time, s3_bucket_name, s3_object_meta
            ))

    return s3_event_records


def check_and_sync_lims_rows():
    """
    Check for LIMS data update and synchronise new data to the db
    """
    client = boto3.client('s3')
    data_object = client.get_object(
        Bucket=os.environ['LIMS_BUCKET_NAME'],
        Key=os.environ['LIMS_CSV_OBJECT_KEY']
    )
    curr_etag = data_object['ETag']

    if not Configuration.same_or_update(name=Configuration.LAST_LIMS_DATA_ETAG, val=curr_etag):
        logger.info("Found new LIMS data, updating")
        with transaction.atomic():
            # Todo: update LIMSRow records
            pass


def sync_s3_event_records(records: List[S3EventRecord]) -> None:
    """
    Synchronise s3 event records to the db.
    :param records: records to be processed
    """

    with transaction.atomic():
        for record in records:
            if record.event_type == EventType.EVENT_OBJECT_REMOVED:
                sync_s3_event_record_removed(record)
            elif record.event_type == EventType.EVENT_OBJECT_CREATED:
                sync_s3_event_record_created(record)
            else:
                logger.error("Found unsupported S3 event type: %s" % record.event_type)


def sync_s3_event_record_removed(record: S3EventRecord):
    """
    Synchronise a S3 event (REMOVED) record to db
    """
    bucket_name = record.s3_bucket_name
    # Removing the matched S3Object
    key = record.s3_object_meta['key']
    logger.info("Deleting an existing S3Object (bucket=%s, key=%s)" % (bucket_name, key))

    s3_object: S3Object = S3Object.objects.filter(bucket=bucket_name, key=key)
    s3_object.delete()


def sync_s3_event_record_created(record: S3EventRecord):
    """
    Synchronise a S3 event (CREATED) record to db
    """
    bucket_name = record.s3_bucket_name

    key = record.s3_object_meta['key']

    size = record.s3_object_meta['size']
    e_tag = record.s3_object_meta['eTag']

    query_set = S3Object.objects.filter(bucket=bucket_name, key=key)
    new = not query_set.exists()
    if new:
        logger.info("Creating a new S3Object (bucket=%s, key=%s)" % (bucket_name, key))
        s3_object = S3Object(
            bucket=bucket_name,
            key=key
        )
    else:
        logger.info("Updating a existing S3Object (bucket=%s, key=%s)" % (bucket_name, key))
        s3_object: S3Object = query_set.get()

    s3_object.size = size
    s3_object.last_modified_date = record.event_time
    s3_object.e_tag = e_tag
    s3_object.save()

    # Find all related LIMS rows and associate them
    lims_rows = LIMSRow.objects.filter(Q(sample_name__in=key) | Q(subject_id__in=key))
    lims_row: LIMSRow
    for lims_row in lims_rows:
        # Create association if not exist
        if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
            logger.info("Linking the S3Object (bucket=%s, key=%s) with LIMSRow (%s)"
                        % (bucket_name, key, str(lims_row)))

            association = S3LIMS(s3_object, lims_row)
            association.save()