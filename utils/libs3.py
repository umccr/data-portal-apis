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

What impl should include in this module:
    Consider, we can take this libs3 to another application and, these impls
    are still meaningful in that totally different application context there!
    Consider, we can package and distribute these impls as standalone SDK/lib.
"""
import gzip
import logging
from datetime import datetime
from enum import Enum

from botocore.exceptions import ClientError

from utils import libaws

logger = logging.getLogger(__name__)


class S3EventType(Enum):
    """
    REF: Supported S3 Event Types
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html
    """
    EVENT_OBJECT_CREATED = "ObjectCreated"
    EVENT_OBJECT_REMOVED = "ObjectRemoved"
    EVENT_UNSUPPORTED = "Unsupported"


def get_s3_uri(bucket, key):
    return f"s3://{bucket}/{key}"


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


def get_s3_object_to_bytes(bucket: str, key: str) -> bytes:
    """
    get_object_to_bytes API, with on-the-fly gzip detection and decompression

    :param bucket:
    :param key:
    :return bytes: bytes from the decompressed S3 object body
    """
    try:
        obj_body = libaws.resource('s3').Object(bucket, key).get()['Body']
        if ".gz" in key:
            obj_bytes = gzip.decompress(obj_body.read())
        else:
            obj_bytes = obj_body.read()
        return obj_bytes
    except ClientError as e:
        message = f"Failed on GET request the specified S3 object (s3://{bucket}/{key})"
        raise FileNotFoundError(message)


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
