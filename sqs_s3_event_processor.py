import os, django
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

migrate.main()


class EventType(Enum):
    EVENT_OBJECT_CREATED = 'ObjectCreated'
    EVENT_OBJECT_REMOVED = 'ObjectRemoved'
    EVENT_UNSUPPORTED = 'Unsupported'


class S3EventRecord:
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
    logging.info("Start processing")
    records = event.get('Records', None)

    if records is None:
        return False

    s3_event_records = []
    for record in records:
        try:
            event_name = record['eventName']
            event_time = parse(record['eventTime'])
            s3 = record['s3']
            s3_bucket_name = s3['bucket']['name']
            s3_object_meta = s3['object']
        except KeyError:
            logging.error("Failed to parse event data")
            return False

        # Check event type
        if EventType.EVENT_OBJECT_CREATED.value in event_name:
            event_type = EventType.EVENT_OBJECT_CREATED
        elif EventType.EVENT_OBJECT_REMOVED.value in event_name:
            event_type = EventType.EVENT_OBJECT_REMOVED
        else:
            event_type = EventType.EVENT_UNSUPPORTED

        logging.info("Found new event of type %s" % event_type)

        s3_event_records.append(S3EventRecord(
            event_type, event_time, s3_bucket_name, s3_object_meta
        ))

    try:
        sync_s3_event_records(s3_event_records)
    except Exception as e:
        logging.error("Rolling back.. an unexpected error occurred: " + str(e))
        return False

    logging.info("Complete")
    return True


def sync_s3_event_records(records: List[S3EventRecord]) -> None:
    """
    Synchronise s3 event records to the db.
    :param records: records to be processed
    """
    import boto3

    bucket_name = os.environ['LIMS_BUCKET_NAME']
    client = boto3.client('s3')
    data_object = client.get_object(
        Bucket=bucket_name,
        Key=os.environ['LIMS_CSV_OBJECT_KEY']
    )
    curr_etag = data_object['ETag']

    if not Configuration.same_or_update(name=Configuration.LAST_LIMS_DATA_ETAG, val=curr_etag):
        logging.info("Found new LIMS data, updating")
        with transaction.atomic():
            # Todo: update LIMSRow records
            pass

    with transaction.atomic():
        for record in records:
            if record.event_type == EventType.EVENT_OBJECT_REMOVED:
                # Removing the matched S3Object
                key = record.s3_object_meta['key']
                logging.info("Deleting an existing S3Object (bucket=%s, key=%s)" % (bucket_name, key))

                s3_object: S3Object = S3Object.objects.filter(bucket=bucket_name, key=key)
                s3_object.delete()
            elif record.event_type == EventType.EVENT_OBJECT_CREATED:
                key = record.s3_object_meta['key']

                size = record.s3_object_meta['size']
                e_tag = record.s3_object_meta['eTag']

                query_set = S3Object.objects.filter(bucket=bucket_name, key=key)
                new = not query_set.exists()
                if new:
                    logging.info("Creating a new S3Object (bucket=%s, key=%s)" % (bucket_name, key))
                    s3_object = S3Object(
                        bucket=bucket_name,
                        key=key
                    )
                else:
                    logging.info("Updating a existing S3Object (bucket=%s, key=%s)" % (bucket_name, key))
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
                        logging.info("Linking the S3Object (bucket=%s, key=%s) with LIMSRow (%s)"
                                     % (bucket_name, key, str(lims_row)))

                        association = S3LIMS(s3_object, lims_row)
                        association.save()
