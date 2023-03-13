# -*- coding: utf-8 -*-
import logging
from typing import List, Dict

import awswrangler as wr

from data_portal.models.workflow import Workflow
from data_portal.models.flowmetrics import FlowMetrics
from data_processors.pipeline.domain.config import SQS_DRACARYS_QUEUE_ARN, S3_DRACARYS_BUCKET_NAME
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    output_json = this_workflow.output
    run_id = this_workflow.portal_run_id
    
    if output_json is None:
        raise RuntimeError('Missing workflow information')
    
    if run_id is None:
        raise RuntimeError('This is not the RunID you are looking for :wave_hand:')

    try:
        lookup_keys = ['multiqc_output_directory']
        multiqc_output_directory = liborca.parse_workflow_output(output_json, lookup_keys)
        outprefix = multiqc_output_directory['nameroot']
        # TODO: Make sure that outprefix matches the destination test object in S3 (see "Test sample comment below")
        multiqc_dir = S3_DRACARYS_BUCKET_NAME/outprefix
        persist_dracarys_data(multiqc_dir, run_id)
        # report_dracarys_data_ingested()

    except KeyError as e:
        logging.info("Dracarys multiqc step didn't find expected multiqc output data. Exiting.")
        return {}


def persist_dracarys_data(multiqc_dir: str, portal_run_id: str):
    ''' Takes Dracarys output files from S3 and turns them into a Dataframe,
        ready to be consumed by the portal DB
    '''
    multiqc_files = wr.s3.list_objects(multiqc_dir)

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

        # Cleanup and match attrs on the model
        df.drop(columns=['sample_id', 'sbj_id']) \
          .rename(columns={'date': 'datetime', 'hash6': 'gds_file_id'})

        obj, created = FlowMetrics.objects.update_or_create(
            portal_run_id=portal_run_id,
            defaults={
                'timestamp': df['timestamp'],
                # 'phenotype': df['phenotype'],
                # 'cov_median_mosdepth': df['cov_median_mosdepth'],
                # 'cov_auto_median_dragen': df['cov_auto_median_dragen'],
                # 'reads_tot_input_dragen': df['reads_tot_input_dragen'],
                # 'reads_mapped_pct_dragen': df['reads_mapped_pct_dragen'],
                # 'insert_len_median_dragen': df['insert_len_median_dragen'],
                # 'var_tot_dragen': df['var_tot_dragen'],
                # 'var_snp_dragen': df['var_snp_dragen'],
                # 'ploidy': df['ploidy'],
                # 'purity': df['purity'],
                # 'qc_status_purple': df['qc_status_purple'],
                # 'sex': df['sex'],
                # 'ms_status': df['ms_status'],
                # 'tmb': df['tmb'],
                # 's3_object_id': df['s3_object_id'],
                # 'gds_file_id': df['gds_file_id']
            }
        )