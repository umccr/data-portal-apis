# -*- coding: utf-8 -*-
# Originally authored by @andrei-seleznev in https://github.com/umccr/dracarys-to-s3-cdk

import logging
from typing import List, Dict

from libumccr import libjson

import awswrangler as wr
import pandas as pd

from data_portal.models.workflow import Workflow
from data.portal.models.libraryrun import LibraryRun
from data.portal.models.limsrow import LIMSRow
from data_portal.models.flowmetrics import FlowMetrics
from data_processors.pipeline.domain.config import SQS_DRACARYS_QUEUE_ARN, S3_DRACARYS_BUCKET_NAME
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    output_json = this_workflow.output
    portal_run_id = this_workflow.portal_run_id

    if output_json is None:
        raise RuntimeError('Missing workflow information')

    if portal_run_id is None:
        raise RuntimeError('This is not the RunID you are looking for :wave_hand:')

    try:
        lookup_keys = ['multiqc_output_directory']
        multiqc_output_directory = liborca.parse_workflow_output(output_json, lookup_keys)
        outprefix = multiqc_output_directory['nameroot']
        # TODO: Make sure that outprefix matches the destination test object in S3 (see "Test sample comment below")
        multiqc_dir = S3_DRACARYS_BUCKET_NAME/outprefix
        # call_dracarys_lambda(multiqc_dir) # report to a SQS

        library_run = LibraryRun.objects.filter(workflows__portal_run_id=portal_run_id)
        assert(library_run.count() == 1) # TODO: Handle multiple libraries case
        library_id = library_run.first().library_id

        subject_id = LIMSRow.objects.filter(library_id=library_id)

        # TODO: Join with supplied Dracarys TSV DF's SBJ_ID?

        persist_dracarys_data(multiqc_dir, portal_run_id) # TODO: Should not be here, calling dracarys lambda should be last thing in this lambda

    except KeyError as e:
        logging.info("Dracarys multiqc step didn't find expected multiqc output data. Exiting.")
        return {}


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
        # Cleanup and match attrs on the model
        df = df.drop(columns = ['sample_id', 'sbj_id']) \
               .rename(columns = {'date': 'datetime', 'hash6': 'athena_job_id'})

        return df

# TODO: Put this on another lambda
def persist_dracarys_data(multiqc_dir: str, portal_run_id: str):
    ''' Takes Dracarys output files from S3 and turns them into a Dataframe,
        ready to be consumed by the portal DB
    '''
    multiqc_files = wr.s3.list_objects(multiqc_dir)

    rows_created = list()
    rows_updated = list()
    rows_invalid = list()

    df = None
    for multiqc_file in multiqc_files:
        logger.info("Processing {}", multiqc_file)

        # Test sample for this at: s3://umccr-research-dev/portal/multiqc/creation_date=2023-02-13/multiqc_results_subset.tsv
        if 'tsv' or 'csv' in multiqc_file:
            df = wr.s3.read_csv(multiqc_file, sep = '\t')
        elif 'parquet' in multiqc_file:
            df = wr.s3.read_parquet(multiqc_file)

        if df is None:
            raise RuntimeError("Failed to serialise multiqc file: {}", multiqc_file)

        # There must be a better way to do this that doesn't involve either
        # .to_sql (and therefore defining the database conn params in code) or
        # going explicit with the attributes of the ORM model vs Dataframe.
        #
        # https://www.laurivan.com/save-pandas-dataframe-as-django-model/

        df = clean_columns(df)

        # Serialise the data into the DB
        for record in df.to_dict('records'):
            try:
                obj, created = FlowMetrics.objects.update_or_create(
                    portal_run_id = portal_run_id,
                    defaults = record
                )

                if created:
                    rows_created.append(obj)
                else:
                    rows_updated.append(obj)

            except Exception as e:
                if any(record.values()):  # silent off iff blank row
                    logger.warning(f"Invalid record: {libjson.dumps(record)} Exception: {e}")
                    rows_invalid.append(record)
                continue

    return {
        'flowmetrics_row_update_count': len(rows_updated),
        'flowmetrics_row_new_count': len(rows_created),
        'flowmetrics_row_invalid_count': len(rows_invalid),
    }