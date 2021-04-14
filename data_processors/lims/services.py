import csv
import io
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Dict

from django.core.exceptions import ValidationError
from django.db import transaction

from data_portal.models import LIMSRow
from data_processors import const
from utils import libgdrive, libssm, libdt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_lims_data_from_google_drive() -> Dict[str, int]:
    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Reading LIMS data from google drive at {requested_time}")

    lims_sheet_id = libssm.get_secret(const.LIMS_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    bytes_data = libgdrive.download_sheet1_csv(
        account_info=account_info,
        file_id=lims_sheet_id,
    )
    csv_input = BytesIO(bytes_data)
    return persist_lims_data(csv_input)


@transaction.atomic
def persist_lims_data(csv_input: BytesIO, rewrite: bool = False) -> Dict[str, int]:
    """
    Persist lims data into the db
    :param csv_input: buffer instance of BytesIO
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
