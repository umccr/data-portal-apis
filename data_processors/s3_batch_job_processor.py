import botocore

try:
  import unzip_requirements
except ImportError:
  pass

import os, django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# All other imports should be placed below
import logging
import boto3
from datetime import datetime
from urllib.parse import unquote

from data_portal.models import S3Object
from data_processors.persist_s3_object import persist_s3_object

RESULT_CODE_SUCCESS = "Succeeded"
RESULT_CODE_TEMP_FAILURE = "TemporaryFailure"
RESULT_CODE_PERM_FAILURE = "PermanentFailure"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info("Processing manifest " + str(event))

    invocation_id = event['invocationId']
    task = event['tasks'][0]
    task_id = task['taskId']
    key = unquote(task['s3Key'])

    # Get bucket arn and convert to bucket name
    bucket_arn: str = task['s3BucketArn']
    tokens = bucket_arn.split(":")
    bucket_name = tokens[-1]

    s3_client = boto3.resource('s3')

    try:
        object_summary = s3_client.ObjectSummary(bucket_name, key).load()
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        logging.error(str(e))

        if error_code == '404':
            response_string = "S3 object (%s, %s) does not exist" % (bucket_name, key)
            return compose_response(invocation_id, task_id, RESULT_CODE_PERM_FAILURE, response_string)

        return compose_response(invocation_id, task_id, RESULT_CODE_TEMP_FAILURE, 'Unknown error. May try again.')

    logging.info("Retrieved S3 meta data: " + str(object_summary))
    e_tag: str = object_summary.e_tag
    last_modified: datetime = object_summary.last_modified
    size: int = object_summary.size

    persist_s3_object(S3Object(
        bucket_name=bucket_name,
        key=key,
        size=size,
        last_modified_date=last_modified,
        e_tag=e_tag
    ))

    return compose_response(
        invocation_id,
        task_id,
        RESULT_CODE_SUCCESS,
        "Successfully added the s3 object information to db"
    )


def compose_response(invocation_id: str, task_id: str, result_code: str, result_string: str):
    return {
        "invocationSchemaVersion": "1.0",
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": invocation_id,
        "results": [
            {
                "taskId": task_id,
                "resultCode": result_code,
                "resultString": result_string
            }
        ]
    }