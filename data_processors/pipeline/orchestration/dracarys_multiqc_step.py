# -*- coding: utf-8 -*-
import logging
from typing import List, Dict

import awswrangler as wr

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_DRACARYS_QUEUE_ARN, S3_DRACARYS_BUCKET_NAME
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    output_json = this_workflow.output
    if output_json is None:
        raise RuntimeError('Missing workflow information')
    
    try:
        lookup_keys = ['multiqc_output_directory']
        multiqc_output_directory = liborca.parse_workflow_output(output_json, lookup_keys)
        outprefix = multiqc_output_directory['nameroot']
        multiqc_files = wr.s3.list_objects(S3_DRACARYS_BUCKET_NAME) # TODO: Work out prefix handling
    except KeyError as e:
        logging.info("Dracarys multiqc step didn't find expected multiqc output data. Exiting.")
        return {}


def persist_dracarys_data(path: str):
    ''' Takes Dracarys output files on S3 and turns them into a Dataframe,
        ready to be consumed by the portal DB
    '''
    if 'tsv' or 'csv' in path:
        wr.s3.read_csv(path)
    elif 'parquet' in path:
        wr.s3.read_parquet(path)