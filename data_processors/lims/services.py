import csv
import io
import logging
import re
from typing import Dict

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q
import operator
import traceback
import pandas as pd
import numpy as np
from collections import namedtuple
from functools import reduce

from data_portal.models import LIMSRow, LabMetadata
from utils import libdt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_lims_data(csv_input: io.BytesIO, rewrite: bool = False) -> Dict[str, int]:
    """
    Persist lims data into the db
    :param csv_input: buffer instance of io.BytesIO
    :param rewrite: whether we are rewriting the data
    :return: result statistics - count of updated, new and invalid LIMS rows
    """
    logger.info(f"Start processing LIMS data")
    csv_reader = csv.DictReader(io.TextIOWrapper(csv_input))

    if rewrite:
        # Delete all rows first
        logger.info("REWRITE MODE: Deleting all existing records")
        LIMSRow.objects.all().delete()

    lims_row_update_count = 0
    lims_row_new_count = 0
    lims_row_invalid_count = 0

    dirty_ids = {}
    na_symbol = "-"  # LIMS Not Applicable symbol is dash

    for row_number, row in enumerate(csv_reader):
        sample_id = row['SampleID']
        illumina_id = row['IlluminaID']
        library_id = row['LibraryID']
        row_id = (illumina_id, library_id)

        if sample_id is None or library_id is None or sample_id == na_symbol or library_id == na_symbol:
            logger.debug(f"Skip row {row_number}. SampleID or LibraryID column is null or NA.")
            lims_row_invalid_count += 1
            continue

        if row_id in dirty_ids:
            prev_row_number = dirty_ids[row_id]
            logger.info(f"Skip row {row_number}. Having duplicated ID with previous row {prev_row_number} "
                        f"on IlluminaID={illumina_id}, LibraryID={library_id}")

            lims_row_invalid_count += 1
            continue

        query_set = LIMSRow.objects.filter(illumina_id=illumina_id, library_id=library_id)

        if not query_set.exists():
            lims_row = __parse_lims_row(row)
            lims_row_new_count += 1
        else:
            lims_row = query_set.get()
            __parse_lims_row(row, lims_row)
            lims_row_update_count += 1

        try:
            lims_row.full_clean()
            lims_row.save()
        except ValidationError as e:
            msg = str(e) + " - " + str(lims_row)
            logger.error(f"Error persisting the LIMS row: {msg}")
            lims_row_invalid_count += 1
            continue

        dirty_ids[row_id] = row_number

    csv_input.close()
    logger.info(f"LIMS data processing complete. "
                f"{lims_row_new_count} new, {lims_row_update_count} updated, {lims_row_invalid_count} invalid")

    return {
        'lims_row_update_count': lims_row_update_count,
        'lims_row_new_count': lims_row_new_count,
        'lims_row_invalid_count': lims_row_invalid_count,
    }


def __parse_lims_row(csv_row: dict, row_object: LIMSRow = None) -> LIMSRow:
    """
    Parse a LIMSRow from a row dict
    :param csv_row: row dict (from csv)
    :param row_object: LIMSRow object, if it is given, it's values will be overwritten
    :return: parsed LIMSRow object
    """
    row_copied = csv_row.copy()

    # Instantiate a new object if provided is None
    lims_row = LIMSRow() if row_object is None else row_object

    for key, value in row_copied.items():
        parsed_value = value.strip()

        # Make sure we dont write in empty strings
        if parsed_value == '-' or value.strip() == '':
            parsed_value = None

        field_name = __csv_column_to_field_name(key)

        if parsed_value is not None:
            # Type conversion for a small number of columns
            if field_name == 'timestamp':
                parsed_value = libdt.parse_lims_timestamp(parsed_value)
            elif field_name == 'run':
                parsed_value = int(parsed_value)

        # Dynamically set field value
        lims_row.__setattr__(field_name, parsed_value)

    return lims_row


def __csv_column_to_field_name(column_name: str) -> str:
    """
    Credit to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    :param column_name: name of the column in CamelCase
    :return: name of the field in snake_case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


@transaction.atomic
def persist_labmetadata(df: pd.DataFrame, rewrite: bool = False) -> Dict[str, int]:
    """
    Persist labmetadata from a pandas dataframe into the db
    :param df: dataframe to persist
    :param rewrite: whether we are rewriting the data
    :return: result statistics - count of updated, new and invalid LabMetadata rows
    """
    df_starting_len = len(df.index)
    logger.info(f"Start processing LabMetadata")
    if rewrite:
        # Delete all rows first
        logger.info("REWRITE MODE: Deleting all existing records")
        LabMetadata.objects.all().delete()

    # clean the DF and reset the index
    df = df.applymap(_clean_datacell)
    df = df.drop_duplicates()
    df = _remove_rows_with_empty_required_cols(df)
    df = df.reset_index(drop=True)

    # Below code assumes a labmetadata's unique identifier is a tuple of library_id and sample_name

    # Assemble a df of entries to update by first querying for existing labmetadatas 
    libids_samplenames = list(zip(df['library_id'].tolist(), df['sample_name'].tolist()))
    query = reduce(operator.or_, (Q(library_id=l, sample_name=s) for l, s in libids_samplenames))
    existing_labmetadatas = LabMetadata.objects.filter(query)
    existing_identifier_tuples = tuple((lm.library_id, lm.sample_name) for lm in existing_labmetadatas)
    df_to_update = df[pd.Series(list(zip(df['library_id'], df['sample_name']))).isin(existing_identifier_tuples)]

    # Assemble df of entries to create.
    df_to_create = df.append(df_to_update)
    df_to_create = df_to_create[~df_to_create.index.duplicated(keep=False)]

    # Create labmetadata ojbects
    try:
        instances_insert = _make_labmetadata_instances_from_dataframe(df_to_create)
        rows_created = LabMetadata.objects.bulk_create(instances_insert, batch_size=100)
    except Exception as e:
        logger.error("Error bulk creating objects! No new rows created")
        logger.error(e)
        logger.debug(traceback.format_exc())
        raise

    # Update labmetadata
    rows_updated = 0
    for row in df_to_update.itertuples():
        rows_updated += _update_labmetadata_instance(row)

    logger.debug("Updated " + str((rows_updated)))
    logger.debug("New " + str(len(rows_created)))
    logger.debug("Invalid " + str(len(df.index) - len(rows_created) - rows_updated))

    return {
        'labmetadata_row_update_count': rows_updated,
        'labmetadata_row_new_count': len(rows_created),
        'labmetadata_row_invalid_count': df_starting_len - len(rows_created) - rows_updated
    }


def _clean_datacell(value):
    # python NaNs are != to themselves
    if value == '-' or value == np.nan or value != value:
        value = ''
    if isinstance(value, str) and value.strip() == '':
        value = ''
    return value


def _remove_rows_with_empty_required_cols(df):
    df.library_id = df.library_id.replace('', np.nan)
    df.sample_id = df.sample_id.replace('', np.nan)
    df.sample_name = df.sample_name.replace('', np.nan)
    df = df.dropna(axis=0, subset=['sample_name', 'library_id', 'sample_id'])
    return df


def _make_labmetadata_instances_from_dataframe(df):
    """
    for each row in dataframe, make a LabMetadata object from it
    :param df: dataframe to turn into LabMetadata object instances
    """
    df_records = df.to_dict('records')

    model_instances = [LabMetadata(
        library_id=record['library_id'],
        sample_name=record['sample_name'],
        sample_id=record['sample_id'],
        external_sample_id=record['external_sample_id'],
        subject_id=record['subject_id'],
        external_subject_id=record['external_subject_id'],
        phenotype=record['phenotype'],
        quality=record['quality'],
        source=record['source'],
        project_name=record['project_name'],
        project_owner=record['project_owner'],
        experiment_id=record['experiment_id'],
        type=record['type'],
        assay=record['assay'],
        override_cycles=record['override_cycles'],
        workflow=record['workflow'],
        coverage=record['coverage'],
        truseqindex=record.get('truseqindex', None)
    ) for record in df_records]
    return model_instances


def _update_labmetadata_instance(row: namedtuple):
    """
    update a single metadata instance determined by library_id and sample_name
    :param row: new parameters to set on a labmetadata that matches row.library_id and row.sample_name
    """
    try:
        return LabMetadata.objects.filter(
            library_id=row.library_id,
            sample_name=row.sample_name,
        ).update(
            library_id=row.library_id,
            sample_name=row.sample_name,
            sample_id=row.sample_id,
            external_sample_id=row.external_sample_id,
            subject_id=row.subject_id,
            external_subject_id=row.external_subject_id,
            phenotype=row.phenotype,
            quality=row.quality,
            source=row.source,
            project_name=row.project_name,
            project_owner=row.project_owner,
            experiment_id=row.experiment_id,
            type=row.type,
            assay=row.assay,
            override_cycles=row.override_cycles,
            workflow=row.workflow,
            coverage=row.coverage,
            truseqindex=row.truseqindex
        )
    except Exception as e:
        msg = f"Error trying to update item with library id {str(row.library_id)} sample name {str(row.sample_name)}"
        logger.error(msg)
        logger.error(e)
        logger.debug(traceback.format_exc())
        raise ()  # bail on error - dont let updates happen
