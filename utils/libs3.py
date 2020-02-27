import logging
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
    Boto head_bucket API
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket

    :param bucket:
    :return: True if bucket exists, False otherwise
    """
    try:
        resp = s3.head_bucket(Bucket=bucket)
        return resp['ResponseMetadata']['HTTPStatusCode'] == 200
    except ClientError as e:
        logger.error(f"Bucket ({bucket}) not found or no permission. Exception: {e}")

    return False
