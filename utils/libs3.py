# -*- coding: utf-8 -*-
"""libs3 module

Module interface for underlay S3 client operations
Loosely based on design patterns: Facade, Adapter/Wrapper

Should retain/suppress all boto S3 API calls here, including
boto specific exceptions and, boto specific data type that need
for processing response.

Goal is, so that else where in code, it does not need to depends on boto3
API directly. i.e. No more import boto3, but just import libs3 instead.

If unsure, start with Pass-through call.
"""
import re
import gzip
import logging
from datetime import datetime

from ast import literal_eval
from botocore.exceptions import ClientError
from enum import Enum
from typing import List, Tuple
from dateutil.parser import parse
from collections import defaultdict

from data_portal.models import S3Object, Report, LIMSRow
from django.core.exceptions import ObjectDoesNotExist

from utils import libaws, libjson

logger = logging.getLogger(__name__)


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


def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.
    https://alexwlchan.net/2019/07/listing-s3-keys/

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = libaws.s3_client().get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix,)
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.
    https://alexwlchan.net/2019/07/listing-s3-keys/

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj["Key"]


def bucket_exists(bucket) -> bool:
    """
    head_bucket API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket

    :param bucket:
    :return bool: True if bucket exists, False otherwise
    """
    try:
        resp = libaws.s3_client().head_bucket(Bucket=bucket)
        return resp['ResponseMetadata']['HTTPStatusCode'] == 200
    except ClientError as e:
        logger.error(f"Bucket ({bucket}) not found or no permission. Exception: {e}")

    return False


def presign_s3_file(bucket: str, key: str) -> (bool, str):
    """
    Generate a presigned URL
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html

    :param bucket:
    :param key:
    :return tuple (bool, str): (true, signed_url) if success, otherwise (false, error message)
    """
    try:
        return True, libaws.s3_client().generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
    except ClientError as e:
        message = f"Failed to sign the specified S3 object (s3://{bucket}/{key}). Exception - {e}"
        logger.error(message)
        return False, message


def head_s3_object(bucket: str, key: str) -> (bool, dict):
    """
    head_object API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    :param bucket:
    :param key:
    :return tuple (bool, dict): (true, dict object metadata) if success, otherwise (false, dict error message)
    """
    try:
        return True, libaws.s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        message = f"Failed on HEAD request the specified S3 object (s3://{bucket}/{key}). Exception - {e}"
        logger.error(message)
        return False, dict(error=message)


def restore_s3_object(bucket: str, key: str, **kwargs) -> (bool, dict):
    """
    restore_object API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.restore_object
    https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html
    https://docs.aws.amazon.com/AmazonS3/latest/dev/restoring-objects.html

    :param bucket:
    :param key:
    :param kwargs:
    :return bool, dict: True if restore request is success, False otherwise with error message
    """
    allowed_tiers = ['Standard', 'Bulk']  # 'Standard' or 'Bulk' for DEEP_ARCHIVE
    tier = kwargs.get('tier', 'Bulk')
    if tier not in allowed_tiers:
        message = f"Failed restore request for the S3 object (s3://{bucket}/{key}). Allow tiers: ({allowed_tiers})"
        logger.error(message)
        return False, dict(error=message)

    days = kwargs.get('days', 7)
    requested_by = kwargs.get('email', '')
    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    restore_request = {'Days': days, 'GlacierJobParameters': {'Tier': tier}}

    try:
        resp = libaws.s3_client().restore_object(Bucket=bucket, Key=key, RestoreRequest=restore_request)
        status_code = resp['ResponseMetadata']['HTTPStatusCode']
        logger.info(f"Requested restore for the S3 object (s3://{bucket}/{key}) with "
                    f"{days} days, {tier} tier, {requested_by} at {requested_time}. "
                    f"Response HTTPStatusCode - {status_code}")
        return True, dict(status_code=status_code)
    except ClientError as e:
        message = f"Failed restore request for the S3 object (s3://{bucket}/{key}). Exception - {e}"
        logger.error(message)
        return False, dict(error=message)


def get_s3_object(bucket: str, key: str, **kwargs) -> (bool, dict):
    """
    get_object API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object

    :param bucket:
    :param key:
    :param kwargs:
    :return tuple (bool, dict): (true, dict object metadata) if success, otherwise (false, dict error message)
    """
    try:
        return True, libaws.s3_client().get_object(Bucket=bucket, Key=key, **kwargs)
    except ClientError as e:
        if e.response['Error']['Code'] == "304":
            return True, e.response
        message = f"Failed on GET request the specified S3 object (s3://{bucket}/{key}). Exception - {e}"
        logger.error(message)
        return False, dict(error=message)


def get_s3_object_to_bytes(bucket: str, key: str, **kwargs) -> bytes:
    """
    get_object_to_bytes API, with on-the-fly gzip detection and decompression

    :param bucket:
    :param key:
    :param kwargs:
    :return tuple (bytes): the bytes from the S3 object body
    """
    obj_body = libaws.resource('s3').Object(bucket, key).get()['Body']
    obj_bytes = gzip.decompress(obj_body.read())
    return libjson.loads(obj_bytes)


def get_s3_object_tagging(bucket: str, key: str):
    """
    get_object_tagging API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging

    TODO Pass-through call

    :param bucket:
    :param key:
    :return:
    """
    return libaws.s3_client().get_object_tagging(Bucket=bucket, Key=key)


def put_s3_object_tagging(bucket: str, key: str, tagging: dict):
    """
    put_object_tagging API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object_tagging

    TODO Pass-through call

    :param bucket:
    :param key:
    :param tagging:
    :return:
    """
    return libaws.s3_client().put_object_tagging(Bucket=bucket, Key=key, Tagging=tagging)


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

            logger.debug(f"Found new event of type {event_type}")

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

    for record in records:
        if record.event_type == S3EventType.EVENT_OBJECT_REMOVED:
            removed_count, records_removed_count = sync_s3_event_record_removed(record)
            results['removed_count'] += removed_count
            results[LIMSRow.__name__+'_removed_count'] += records_removed_count

        elif record.event_type == S3EventType.EVENT_OBJECT_CREATED:
            created_count, records_created_count = sync_s3_event_record_created(record)
            results['created_count'] += created_count
            results[LIMSRow.__name__+'_created_count'] += records_created_count
        else:
            logger.info(f"Found unsupported S3 event type: {record.event_type}")
            results['unsupported_count'] += 1

    return results


def delete_s3_object(bucket_name: str, key: str) -> Tuple[int, int]:
    """
    Delete a S3 object record from db
    :param bucket_name: s3 bucket name
    :param key: s3 object key
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    try:
        s3_object: S3Object = S3Object.objects.get(bucket=bucket_name, key=key)
        records = S3Object.objects.filter(s3_object=s3_object)
        records_count = records.count()
        records.delete()
        s3_object.delete()
        logger.info(f"Deleted S3Object: s3://{bucket_name}/{key}")
        return 1, records_count
    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent S3Object (bucket={bucket_name}, key={key}): {str(e)}")
        return 0, 0


def sync_s3_event_record_removed(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (REMOVED) record to db
    :param record: record to be synced
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    return delete_s3_object(bucket_name, key)


def persist_s3_object(bucket: str, key: str, last_modified_date: datetime, size: int, e_tag: str) -> Tuple[int, int]:
    """
    Persist an s3 object record into the db
    :param bucket: s3 bucket name
    :param key: s3 object key
    :param last_modified_date: s3 object last modified date
    :param size: s3 object size
    :param e_tag: s3 objec etag
    :return: number of s3 object created, number of s3-lims association records created
    """
    query_set = S3Object.objects.filter(bucket=bucket, key=key)
    new = not query_set.exists()

    if new:
        logger.info(f"Creating a new S3Object (bucket={bucket}, key={key})")
        s3_object = S3Object(
            bucket=bucket,
            key=key
        )
    else:
        logger.info(f"Updating a existing S3Object (bucket={bucket}, key={key})")
        s3_object: S3Object = query_set.get()

    s3_object.last_modified_date = last_modified_date
    s3_object.size = size
    s3_object.e_tag = e_tag
    s3_object.save()

    if not new:
        return 0, 0

    # TODO: Also persist accompanying LIMS ORM objects elsewhere, not here, part of s3/services.py refactor
    #lims_assoc_rows()


def sync_s3_event_record_created(record: S3EventRecord) -> Tuple[int, int]:
    """
    Synchronise a S3 event (CREATED) record to db
    :return: number of s3 object created, number of database association records created
    """
    bucket_name = record.s3_bucket_name
    key = record.s3_object_meta['key']
    size = record.s3_object_meta['size']
    e_tag = record.s3_object_meta['eTag']

    tag_s3_object(bucket_name, key, "bam")

    return persist_s3_object(
        bucket=bucket_name, key=key, size=size, last_modified_date=record.event_time, e_tag=e_tag
    )


def delete_s3_object(bucket_name: str, key: str) -> Tuple[int, int]:
    """
    Delete a S3 object record from db
    :param bucket_name: s3 bucket name
    :param key: s3 object key
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    try:
        s3_object: S3Object = S3Object.objects.get(bucket=bucket_name, key=key)
        records = S3Object.objects.filter(s3_object=s3_object)
        records_count = records.count()
        records.delete()
        s3_object.delete()
        logger.info(f"Deleted S3Object: s3://{bucket_name}/{key}")
        return 1, records_count
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
        response = get_s3_object_tagging(bucket=bucket_name, key=key)
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

        payload = put_s3_object_tagging(bucket=bucket_name, key=key, tagging={'TagSet': tag_set})

        if payload['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"Tagged the S3Object ({key}) with ({str(tag_set)})")
        else:
            logger.error(f"Failed to tag the S3Object ({key}) with ({str(payload)})")

    else:
        # sound of silence
        pass
