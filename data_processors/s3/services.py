# -*- coding: utf-8 -*-
"""s3 object services module
Impl in here contains some transactional boundary or business logic
that specific to this application context and package purpose. Typically,
some logic to be applied or called at this service layer, before hitting
to database for persisting. May involve concerting one or more of backing
Models or entities, in this case, our S3Object model, and/or some aspect of
direct mutation to the actual S3 object itself, etc.
"""
import logging
from collections import defaultdict
from datetime import datetime
from typing import Tuple, List

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
# from django.db.models import ExpressionWrapper, Value, CharField, Q, F  FIXME to be removed when refactoring #343
from libumccr.aws import libs3

from data_portal.fields import HashFieldHelper
from data_portal.models.limsrow import LIMSRow, S3LIMS
from data_portal.models.s3object import S3Object
from data_processors.const import S3EventRecord

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def sync_s3_event_records(records: List[S3EventRecord]) -> dict:
    """
    Synchronise s3 event records to the db.
    :param records: records to be processed
    :return results of synchronisation
    """
    results = defaultdict(int)

    obj_list = list()
    for record in records:
        if record.event_type == libs3.S3EventType.EVENT_OBJECT_REMOVED:
            removed_count, s3_lims_removed_count = _sync_s3_event_record_removed(record)
            results['removed_count'] += removed_count
            results['s3_lims_removed_count'] += s3_lims_removed_count

        elif record.event_type == libs3.S3EventType.EVENT_OBJECT_CREATED:
            # created_count, s3_lims_created_count = _sync_s3_event_record_created(record)
            obj_list.append(_sync_s3_event_record_created(record))
            created_count, s3_lims_created_count = (1, 1)
            results['created_count'] += created_count
            results['s3_lims_created_count'] += s3_lims_created_count
        else:
            logger.info(f"Found unsupported S3 event type: {record.event_type}")
            results['unsupported_count'] += 1

    persist_s3_object_bulk(obj_list)

    return results


@transaction.atomic
def persist_s3_object_bulk(obj_list):
    S3Object.objects.bulk_create(
        obj_list,
        update_conflicts=True,
        # unique_fields=['unique_hash'],
        update_fields=['last_modified_date', 'size', 'e_tag'],
    )


def _sync_s3_event_record_created(record: S3EventRecord) -> S3Object:
    """
    Synchronise a S3 event (CREATED) record to db
    :return: number of s3 object created, number of database association records created
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    size = record.s3_object_meta['size']

    if 'eTag' in record.s3_object_meta:
        e_tag = record.s3_object_meta['eTag']  # S3 Event Notification convention
    elif 'etag' in record.s3_object_meta:
        e_tag = record.s3_object_meta['etag']  # EventBridge convention
    else:
        e_tag = None

    # tag_s3_object(bucket_name, key, "bam")

    return persist_s3_object(bucket=bucket_name, key=key, size=size, last_modified_date=record.event_time, e_tag=e_tag)


def _sync_s3_event_record_removed(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (REMOVED) record to db
    :param record: record to be synced
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    return delete_s3_object(bucket_name, key)


# @transaction.atomic
def persist_s3_object(bucket: str, key: str, last_modified_date: datetime, size: int, e_tag: str) -> S3Object:
    """
    Persist an s3 object record into the db
    :param bucket: s3 bucket name
    :param key: s3 object key
    :param last_modified_date: s3 object last modified date
    :param size: s3 object size
    :param e_tag: s3 objec etag
    :return: number of s3 object created, number of s3-lims association records created
    """
    # query_set = S3Object.objects.filter(bucket=bucket, key=key)
    # new = not query_set.exists()

    # if new:
    #     logger.info(f"Creating a new S3Object (bucket={bucket}, key={key})")
    #     s3_object = S3Object(
    #         bucket=bucket,
    #         key=key
    #     )
    # else:
    #     logger.info(f"Updating a existing S3Object (bucket={bucket}, key={key})")
    #     s3_object: S3Object = query_set.get()

    # s3_object.last_modified_date = last_modified_date
    # s3_object.size = size
    # s3_object.e_tag = e_tag
    # s3_object.save()

    logger.info(f"Upsert S3Object (bucket={bucket}, key={key})")
    s3_object = S3Object(
        bucket=bucket,
        key=key,
        last_modified_date=last_modified_date,
        size=size,
        e_tag=e_tag,
    )
    return s3_object

    # if not new:
    #     return 0, 0

    # TODO remove association logic and drop S3LIMS table, related with global search overhaul
    #  see https://github.com/umccr/data-portal-apis/issues/343
    # Number of s3-lims association records we have created in this run
    # new_association_count = 0

    # FIXME quick patch fix, permanently remove these when refactoring #343 in next iteration
    #  commented out the following association link due to performance issue upon S3 object Update events
    #  see https://github.com/umccr/data-portal-apis/issues/143
    #
    # # Find all related LIMS rows and associate them
    # # Credit: https://stackoverflow.com/questions/49622088/django-filtering-queryset-by-parameter-has-part-of-fields-value
    # # If the linking columns have changed, we need to modify
    # key_param = ExpressionWrapper(Value(key), output_field=CharField())
    #
    # # For each attr (values), s3 object key should contain it
    # attr_filter = Q()
    # # AND all filters
    # for attr in LIMSRow.S3_LINK_ATTRS:
    #     attr_filter &= Q(param__contains=F(attr))
    #
    # lims_rows = LIMSRow.objects.annotate(param=key_param).filter(attr_filter)
    # lims_row: LIMSRow
    # for lims_row in lims_rows:
    #     # Create association if not exist
    #     if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
    #         logger.info(f"Linking the S3Object ({str(s3_object)}) with LIMSRow ({str(lims_row)})")
    #
    #         association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
    #         association.save()
    #
    #         new_association_count += 1
    #
    # # Check if we do find any association at all or not
    # if len(lims_rows) == 0:
    #     logger.debug(f"No association to any LIMS row is found for the S3Object (bucket={bucket}, key={key})")

    # return 1, new_association_count


@transaction.atomic
def delete_s3_object(bucket_name: str, key: str) -> Tuple[int, int]:
    """
    Delete a S3 object record from db
    :param bucket_name: s3 bucket name
    :param key: s3 object key
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    try:
        h = HashFieldHelper()
        h.add(bucket_name).add(key)
        hash_key_lookup = h.calculate_hash()
        s3_object = S3Object.objects.filter(unique_hash__exact=hash_key_lookup)

        # s3_object: S3Object = S3Object.objects.get(bucket=bucket_name, key=key)

        # TODO remove association logic and drop S3LIMS table, related with global search overhaul
        #  see https://github.com/umccr/data-portal-apis/issues/343
        #
        # s3_lims_records = S3LIMS.objects.filter(s3_object=s3_object)
        # s3_lims_count = s3_lims_records.count()
        # s3_lims_records.delete()

        if s3_object.exists():
            s3_object.delete()
            logger.info(f"Deleted S3Object: s3://{bucket_name}/{key}")
            return 1, 0
        else:
            logger.info(f"No deletion required. Non-existent S3Object (bucket={bucket_name}, key={key})")
            return 0, 0
    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent S3Object (bucket={bucket_name}, key={key}): {str(e)}")
        return 0, 0


def tag_s3_object(bucket_name: str, key: str, extension: str):
    """
    Tag S3 Object if extension is <extension>

    NOTE: You can associate up to 10 tags with an object. See
    https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
    :param bucket_name:
    :param key:
    :param extension: File type or extension without dots, i.e: "bam", "vcf", "tsv", "csv", etc...
    """

    if key.endswith(extension):
        response = libs3.get_s3_object_tagging(bucket=bucket_name, key=key)
        tag_set = response.get('TagSet', [])

        tag_archive = {'Key': 'Archive', 'Value': 'true'}
        tag_extension = {'Key': 'Filetype', 'Value': extension}

        if len(tag_set) == 0:
            tag_set.append(tag_archive)
            tag_set.append(tag_extension)
        else:
            # have existing tags
            immutable_tags = tuple(tag_set)  # have immutable copy first
            if tag_extension not in immutable_tags:
                tag_set.append(tag_extension)  # just add tag_bam
            if tag_archive not in immutable_tags:
                values = set()
                for tag in immutable_tags:
                    for value in tag.values():
                        values.add(value)
                if tag_archive['Key'] not in values:
                    tag_set.append(tag_archive)  # only add if Archive is not present

        payload = libs3.put_s3_object_tagging(bucket=bucket_name, key=key, tagging={'TagSet': tag_set})

        if payload['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"Tagged the S3Object ({key}) with ({str(tag_set)})")
        else:
            logger.error(f"Failed to tag the S3Object ({key}) with ({str(payload)})")

    else:
        # sound of silence
        pass
