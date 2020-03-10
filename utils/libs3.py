import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()

s3 = boto3.client("s3")


def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    NOTE: For big bucket, it will be paginated by 1000 objects per request.
    e.g. 450,000 objects  * $0.0055 / 1000 = $2.475

    Generate objects in an S3 bucket.
    https://alexwlchan.net/2019/07/listing-s3-keys/

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = s3.get_paginator("list_objects_v2")

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
        resp = s3.head_bucket(Bucket=bucket)
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
        return True, s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
    except ClientError as e:
        message = f"Failed to sign the specified S3 object (s3://{bucket}/{key}). Exception - {e}"
        logging.error(message)
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
        return True, s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        message = f"Failed head request the specified S3 object (s3://{bucket}/{key}). Exception - {e}"
        logging.error(message)
        return False, dict(error=message)


def restore_s3_object(bucket: str, key: str, **kwargs) -> (bool, dict):
    """
    restore_object API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.restore_object
    https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html
    https://docs.aws.amazon.com/AmazonS3/latest/dev/restoring-objects.html

    :param bucket:
    :param key:
    :param days:
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
        resp = s3.restore_object(Bucket=bucket, Key=key, RestoreRequest=restore_request)
        status_code = resp['ResponseMetadata']['HTTPStatusCode']
        logger.info(f"Requested restore for the S3 object (s3://{bucket}{key}) with "
                    f"{days} days, {tier} tier, {requested_by} at {requested_time}. "
                    f"Response HTTPStatusCode - {status_code}")
        return True, dict(status_code=status_code)
    except ClientError as e:
        message = f"Failed restore request for the S3 object (s3://{bucket}/{key}). Exception - {e}"
        logger.error(message)
        return False, dict(error=message)
