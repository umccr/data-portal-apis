import logging
import re

import numpy as np
import pandas as pd
from django.db import transaction

from data_portal.models.labmetadata import LabMetadata
from data_processors import const
from utils import libssm, libgdrive, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def download_metadata(year: str) -> pd.DataFrame:
    """Download the full original metadata from which to extract the required information

    :param year: the sheet in the metadata spreadsheet to load
    """
    lab_sheet_id = libssm.get_secret(const.TRACKING_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    return libgdrive.download_sheet(account_info, lab_sheet_id, sheet=year)


def truncate_labmetadata():
    """DDL TRUNCATE TABLE is implicit commit, hence, keep default autocommit for transaction"""
    tbl_name = LabMetadata.get_table_name()
    try:
        LabMetadata.truncate()
        logger.info(f"Truncating '{tbl_name}' table succeeded")
        return True
    except Exception as e:
        logger.error(f"Error truncating '{tbl_name}' table. Exception: {e}")
        return False


@transaction.atomic
def persist_labmetadata(df: pd.DataFrame):
    """
    Persist labmetadata from a pandas dataframe into the db

    Note that if table is truncated prior calling this then 'create' is implicit

    :param df: dataframe to persist
    :return: result statistics - count of LabMetadata rows created
    """
    logger.info(f"Start processing LabMetadata")

    if df.empty:
        return {
            'message': "Empty data frame"
        }

    df = clean_columns(df)
    df = df.applymap(_clean_data_cell)
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    rows_created = list()
    rows_updated = list()
    rows_invalid = list()

    for record in df.to_dict('records'):
        library_id = record.get('library_id') or None
        try:
            obj, created = LabMetadata.objects.update_or_create(
                library_id=library_id,
                defaults={
                    'library_id': library_id,
                    'sample_name': record.get('sample_name') or None,
                    'sample_id': record.get('sample_id') or None,
                    'external_sample_id': record['external_sample_id'],
                    'subject_id': record['subject_id'],
                    'external_subject_id': record['external_subject_id'],
                    'phenotype': record['phenotype'],
                    'quality': record['quality'],
                    'source': record['source'],
                    'project_name': record['project_name'],
                    'project_owner': record['project_owner'],
                    'experiment_id': record['experiment_id'],
                    'type': record['type'],
                    'assay': record['assay'],
                    'override_cycles': record['override_cycles'],
                    'workflow': record['workflow'],
                    'coverage': record['coverage'],
                    'truseqindex': record.get('truseqindex', None),
                }
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
        'labmetadata_row_update_count': len(rows_updated),
        'labmetadata_row_new_count': len(rows_created),
        'labmetadata_row_invalid_count': len(rows_invalid),
    }


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    clean a dataframe of labmetadata from a tracking sheet to correspond to the django object model
    we do this by editing the columns to match the django object
    """
    # remove unnamed
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # simplify verbose column names
    df = df.rename(columns={'Coverage (X)': 'coverage', "TruSeq Index, unless stated": "truseqindex"})

    # convert PascalCase headers to snake_case and fix ID going to _i_d
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    df = df.rename(columns=lambda x: pattern.sub('_', x).lower().replace('_i_d', '_id'))

    return df


def _clean_data_cell(value):
    # python NaNs are != to themselves
    if value == '_' or value == '-' or value == np.nan or value != value:
        value = ''
    if isinstance(value, str) and value.strip() == '':
        value = ''
    return value
