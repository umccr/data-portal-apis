import csv
import io
import logging
import re

import boto3
from botocore.response import StreamingBody
from django.core.exceptions import ValidationError
from django.db import transaction, IntegrityError
from django.db.models import Q
from data_portal.models import LIMSRow, S3Object, S3LIMS
from utils.datetime import parse_lims_timestamp

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UnexpectedLIMSDataFormatException(Exception):
    """
    Raised when we encounter unexpected LIMS data format, such as duplicate row identifiers
    """
    def __init__(self, message) -> None:
        super().__init__('Unexpected LIMS data format - ' + message)


@transaction.atomic  # Either the whole csv will be processed without error; or no data will be updated!
def persist_lims_data(csv_bucket: str, csv_key: str, rewrite: bool = False):
    """
    Persist lims data into the db
    :param csv_bucket: the s3 bucket storing the csv file
    :param csv_key: the s3 file key of the csv
    :param rewrite: whether we are rewriting the data
    :param force_insert: whether we are force inserting data into the db, regardless of duplicate id
    :return:
    """
    client = boto3.client('s3')
    # Note that the body data is lazy loaded
    data_object = client.get_object(
        Bucket=csv_bucket,
        Key=csv_key
    )

    logger.info('Reading csv data')
    body: StreamingBody = data_object['Body']
    bytes_data = body.read()

    csv_input = io.BytesIO(bytes_data)
    csv_reader = csv.DictReader(io.TextIOWrapper(csv_input))

    if rewrite:
        # Delete all rows (and associations) first
        logger.info("REWRITE MODE: Deleting all existing records")
        LIMSRow.objects.all().delete()

    lims_row_update_count = 0
    lims_row_new_count = 0
    lims_row_invalid_count = 0
    association_count = 0

    dirty_ids = {}

    for row_number, row in enumerate(csv_reader):
        try:
            lims_row, new = parse_and_persist_lims_object(dirty_ids, row, row_number)
        except UnexpectedLIMSDataFormatException as e:
            # Report an error instead of let the whole transaction fails
            logger.error("Error persisting the LIMS row: " + str(e))
            lims_row_invalid_count += 1
            continue

        if new:
            lims_row_new_count += 1
        else:
            lims_row_update_count += 1

        # Only find association if we have SubjectID, as it can be None
        if lims_row.subject_id is not None:
            # Find all matching S3Object objects and create association between them and the lims row
            key_filter = Q()

            # AND all filters
            for attr in LIMSRow.S3_LINK_ATTRS:
                key_filter &= Q(key__contains=getattr(lims_row, attr))

            for s3_object in S3Object.objects.filter(key_filter):
                # Create association if not exist
                if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
                    logger.info(f"Linking the S3Object ({str(s3_object)}) with LIMSRow ({str(lims_row)})")

                    association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
                    association.save()

                    association_count += 1

    csv_input.close()
    logger.info(f'LIMS data processing complete. \n'
                f'{lims_row_new_count} new, {lims_row_update_count} updated, {lims_row_invalid_count} invalid, \n'
                f'{association_count} new associations')

    return {
        'lims_row_update_count': lims_row_update_count,
        'lims_row_new_count': lims_row_new_count,
        'lims_row_invalid_count': failed_count,
        'association_count': association_count,
    }


def parse_and_persist_lims_object(dirty_ids: dict, row: dict, row_number: int):
    """
    Parse and persist the LIMSRow from the row dict to the db
    :param dirty_ids: used to keep track of dirty row (identifiers)
    :param row: row dict (from csv)
    :param row_number: index of the row
    :return: saved LIMSRow, a flag indicating whether the object is newly created
    """

    # Using the identifier combination to find the object
    illumina_id = row['IlluminaID']
    library_id = row['LibraryID']
    row_id = (illumina_id, library_id)

    # If find another row in which the id has been seen in previous rows, we raise an error
    if row_id in dirty_ids:
        prev_row_number = dirty_ids[row_id]
        raise UnexpectedLIMSDataFormatException(f'Duplicate row identifier for row {prev_row_number} and {row_number}:'
                                                f'IlluminaID={illumina_id}, LibraryID={library_id}')

    query_set = LIMSRow.objects.filter(illumina_id=illumina_id, library_id=library_id)

    if not query_set.exists():
        lims_row = parse_lims_row(row)
        new = True
    else:
        lims_row = query_set.get()
        parse_lims_row(row, lims_row)
        new = False

    try:
        lims_row.full_clean()
        lims_row.save()
    except IntegrityError as e:
        raise UnexpectedLIMSDataFormatException(str(e))
    except ValidationError as e:
        raise UnexpectedLIMSDataFormatException(str(e))

    # Mark this row as dirty
    dirty_ids[row_id] = row_number

    return lims_row, new


def parse_lims_row(csv_row: dict, row_object: LIMSRow = None) -> LIMSRow:
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

        field_name = csv_column_to_field_name(key)

        if parsed_value is not None:
            # Type conversion for a small number of columns
            if field_name == 'timestamp':
                parsed_value = parse_lims_timestamp(parsed_value)
            elif field_name == 'run':
                parsed_value = int(parsed_value)

        # Dynamically set field value
        lims_row.__setattr__(field_name, parsed_value)

    return lims_row


def csv_column_to_field_name(column_name: str) -> str:
    """
    Credit to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    :param column_name: name of the column in CamelCase
    :return: name of the field in snake_case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
