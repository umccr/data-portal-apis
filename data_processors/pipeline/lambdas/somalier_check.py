# -*- coding: utf-8 -*-
try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
from urllib.parse import urlparse
from datetime import datetime

from libumccr import libjson

from data_processors.pipeline.domain.somalier import HolmesPipeline, HolmesCheckDto

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context) -> dict:
    """event payload dict
    {
        "index": "gds://volume/path/to/this.bam"       (MANDATORY)
    }

    Given gds path, find the closest related samples

    :param event:
    :param context:
    :return: fastq container
    """

    logger.info(f"Start checking index")
    logger.info(libjson.dumps(event))

    index: str = event['index']

    # Get name
    timestamp = int(datetime.now().replace(microsecond=0).timestamp())
    step_function_instance_name = "__".join([
        "somalier_check",
        urlparse(index).path.lstrip("/").replace("/", "_").rstrip(".bam")[-40:],
        str(timestamp)
    ])

    dto = HolmesCheckDto(
        run_name=step_function_instance_name,
        indexes=[index],
    )

    holmes_pipeline = (
        HolmesPipeline()
        .check(dto)
        .poll()
    )

    # Return execution result as-is for now
    return libjson.loads(libjson.dumps(holmes_pipeline.execution_result))
