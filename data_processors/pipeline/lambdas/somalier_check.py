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

    dto = HolmesCheckDto(
        run_name=HolmesPipeline.get_step_function_instance_name(prefix="somalier_check", index=index),
        indexes=[index],
    )

    holmes_pipeline = (
        HolmesPipeline()
        .check(dto)
        .poll()
    )

    # Return execution result as-is for now
    return libjson.loads(libjson.dumps(holmes_pipeline.execution_result))
