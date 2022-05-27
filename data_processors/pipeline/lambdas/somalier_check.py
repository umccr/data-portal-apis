#!/usr/bin/env python3

try:
    import unzip_requirements
except ImportError:
    pass

import django
import os
from data_processors.pipeline.services.somalier_check_srv import \
    get_fingerprint_check_service_instance
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime

from time import sleep
from typing import List
from libumccr import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context) -> List:
    """event payload dict
    {
        "index": "gds://path/to/volume"
    }

    Given a gds path, find the closest related samples

    :param event:
    :param context:
    :return: fastq container
    """

    logger.info(f"Start checking index")
    logger.info(libjson.dumps(event))

    gds_path: str = event['index']

    # Call somalier check step
    somalier_check_step_function = get_fingerprint_check_service_instance()

    # Get name
    timestamp = int(datetime.now().replace(microsecond=0).timestamp())
    step_function_instance_name = "__".join([
        "somalier_check",
        urlparse(gds_path).path.lstrip("/").replace("/", "_").rstrip(".bam")[-40:],
        str(timestamp)
    ])

    # Call check step function
    client = boto3.client('stepfunctions')
    step_function_instance_obj = client.start_execution(
        stateMachineArn=somalier_check_step_function,
        name=step_function_instance_name,
        input=json.dumps(
            {
                "index": gds_path
            }
        )
    )

    # Get execution arn
    somalier_check_execution_arn = step_function_instance_obj.get('executionArn', None)
    if somalier_check_execution_arn is None:
        logger.warning("Could not get somalier check execution arg")
        return None

    running_status = "RUNNING"
    while True:
        execution_dict = client.describe_execution(
            executionArn=somalier_check_execution_arn
        )
        status = execution_dict.get("status", None)
        if status is None:
            logger.warning("Could not get status of somalier check execution")
            return None
        if status != running_status:
            break
        logger.info(f"Execution still running, sleeping 3")
        sleep(3)

    # Return output
    return json.loads(execution_dict.get("output"))
