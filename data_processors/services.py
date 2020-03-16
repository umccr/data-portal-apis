import csv
import io
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Tuple, Dict

from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction, IntegrityError
from django.db.models import Q, ExpressionWrapper, Value, CharField, F

from data_portal.models import S3Object, LIMSRow, S3LIMS
from data_processors.exceptions import UnexpectedLIMSDataFormatException
from utils.datetime import parse_lims_timestamp
from utils import libgdrive, libssm, libs3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_s3_object(bucket: str, key: str, last_modified_date: datetime, size: int, e_tag: str) -> Tuple[int, int]:
    """
    Persist an s3 object record into the db
    :param bucket: s3 bucket name
    :param key: s3 object key
    :param last_modified_date: s3 object last modified date
    :param size: s3 object size
    :param e_tag: s3 objec etag
    :return: number of s3 object created, number of s3-lims association records created
    """
    query_set = S3Object.objects.filter(bucket=bucket, key=key)
    new = not query_set.exists()

    if new:
        logger.info(f"Creating a new S3Object (bucket={bucket}, key={key})")
        s3_object = S3Object(
            bucket=bucket,
            key=key
        )
    else:
        logger.info(f"Updating a existing S3Object (bucket={bucket}, key={key})")
        s3_object: S3Object = query_set.get()

    s3_object.last_modified_date = last_modified_date
    s3_object.size = size
    s3_object.e_tag = e_tag
    s3_object.save()

    if not new:
        return 0, 0

    # Number of s3-lims association records we have created in this run
    new_association_count = 0

    # Find all related LIMS rows and associate them
    # Credit: https://stackoverflow.com/questions/49622088/django-filtering-queryset-by-parameter-has-part-of-fields-value
    # If the linking columns have changed, we need to modify
    key_param = ExpressionWrapper(Value(key), output_field=CharField())

    # For each attr (values), s3 object key should contain it
    attr_filter = Q()
    # AND all filters
    for attr in LIMSRow.S3_LINK_ATTRS:
        attr_filter &= Q(param__contains=F(attr))

    lims_rows = LIMSRow.objects.annotate(param=key_param).filter(attr_filter)
    lims_row: LIMSRow
    for lims_row in lims_rows:
        # Create association if not exist
        if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
            logger.info(f"Linking the S3Object ({str(s3_object)}) with LIMSRow ({str(lims_row)})")

            association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
            association.save()

            new_association_count += 1

    # Check if we do find any association at all or not
    if len(lims_rows) == 0:
        logging.error(f"No association to any LIMS row is found for the S3Object (bucket={bucket}, key={key})")

    return 1, new_association_count


def delete_s3_object(bucket_name: str, key: str) -> Tuple[int, int]:
    """
    Delete a S3 object record from db
    :param bucket_name: s3 bucket name
    :param key: s3 object key
    :return: number of s3 records deleted, number of s3-lims association records deleted
    """
    try:
        s3_object: S3Object = S3Object.objects.get(bucket=bucket_name, key=key)
        s3_lims_records = S3LIMS.objects.filter(s3_object=s3_object)
        s3_lims_count = s3_lims_records.count()
        s3_lims_records.delete()
        s3_object.delete()
        return 1, s3_lims_count
    except ObjectDoesNotExist as e:
        logger.error("Failed to remove an in-existent S3Object record: " + str(e))
        return 0, 0


def tag_s3_object(bucket_name: str, key: str):
    """
    Tag S3 Object if extension is .bam

    NOTE: You can associate up to 10 tags with an object. See
    https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
    :param bucket_name:
    :param key:
    """

    if key.endswith('.bam'):
        response = libs3.get_s3_object_tagging(bucket=bucket_name, key=key)
        tag_set = response.get('TagSet', [])

        tag_archive = {'Key': 'Archive', 'Value': 'true'}
        tag_bam = {'Key': 'Filetype', 'Value': 'bam'}

        if len(tag_set) == 0:
            tag_set.append(tag_archive)
            tag_set.append(tag_bam)
        else:
            # have existing tags
            immutable_tags = tuple(tag_set)  # have immutable copy first
            if tag_bam not in immutable_tags:
                tag_set.append(tag_bam)  # just add tag_bam
            if tag_archive not in immutable_tags:
                values = set()
                for tag in immutable_tags:
                    for value in tag.values():
                        values.add(value)
                if tag_archive['Key'] not in values:
                    tag_set.append(tag_archive)  # only add if Archive is not present

        payload = libs3.put_s3_object_tagging(bucket=bucket_name, key=key, tagging={'TagSet': tag_set})

        if payload['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"Tagged the S3Object ({key}) with ({str(tag_set)})")
        else:
            logger.error(f"Failed to Tag the S3Object ({key}) with ({str(payload)})")

    else:
        # sound of silence
        pass


@transaction.atomic
def persist_lims_data(csv_bucket: str, csv_key: str, rewrite: bool = False, create_association: bool = False) -> Dict[str, int]:
    """
    Persist lims data into the db
    :param csv_bucket: the s3 bucket storing the csv file
    :param csv_key: the s3 file key of the csv
    :param rewrite: whether we are rewriting the data
    :param create_association: whether to create association link between LIMS rows and S3 object
    :return: result statistics - count of updated, new and invalid LIMS rows and new S3LIMS associations
    """
    logger.info("Reading LIMS data from bucket")
    bytes_data = libs3.read_s3_object_body(csv_bucket, csv_key)
    csv_input = BytesIO(bytes_data)
    return __persist_lims_data(csv_input, rewrite, create_association)


@transaction.atomic
def persist_lims_data_from_google_drive(account_info_ssm_key: str, file_id_ssm_key: str) -> Dict[str, int]:
    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Reading LIMS data from drive at {requested_time}")

    bytes_data = libgdrive.download_sheet1_csv(
        account_info=libssm.get_secret(account_info_ssm_key),
        file_id=libssm.get_secret(file_id_ssm_key),
    )
    csv_input = BytesIO(bytes_data)
    return __persist_lims_data(csv_input)


@transaction.atomic  # Either the whole csv will be processed without error; or no data will be updated!
def __persist_lims_data(csv_input: BytesIO, rewrite: bool = False, create_association: bool = False) -> Dict[str, int]:
    """
    Persist lims data into the db
    :param csv_input: buffer instance of BytesIO
    :param rewrite: whether we are rewriting the data
    :param create_association: whether to create association link between LIMS rows and S3 object
    :return: result statistics - count of updated, new and invalid LIMS rows and new S3LIMS associations
    """
    logger.info(f"Start processing LIMS data...")
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
            lims_row, new = __parse_and_persist_lims_object(dirty_ids, row, row_number)
        except UnexpectedLIMSDataFormatException as e:
            # Report an error instead of let the whole transaction fails
            msg = str(e)
            # TODO skipping known issue logging for now
            if not msg.find("{'sample_id': ['This field cannot be null.']}") or \
                    not msg.find("{'library_id': ['This field cannot be null.']}"):
                logger.error("Error persisting the LIMS row: " + msg)
            lims_row_invalid_count += 1
            continue

        if new:
            lims_row_new_count += 1
        else:
            lims_row_update_count += 1

        # Only find association if we have SubjectID, as it can be None
        if lims_row.subject_id is not None and create_association:
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
        'lims_row_invalid_count': lims_row_invalid_count,
        'association_count': association_count,
    }


def __parse_and_persist_lims_object(dirty_ids: dict, row: dict, row_number: int) -> Tuple[LIMSRow, bool]:
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
        msg = f'Duplicate row identifier for row {prev_row_number} and {row_number}: ' \
              f'IlluminaID={illumina_id}, LibraryID={library_id}'
        raise UnexpectedLIMSDataFormatException(msg)

    query_set = LIMSRow.objects.filter(illumina_id=illumina_id, library_id=library_id)

    if not query_set.exists():
        lims_row = __parse_lims_row(row)
        new = True
    else:
        lims_row = query_set.get()
        __parse_lims_row(row, lims_row)
        new = False

    try:
        lims_row.full_clean()
        lims_row.save()
    except IntegrityError as e:
        raise UnexpectedLIMSDataFormatException(str(e))
    except ValidationError as e:
        raise UnexpectedLIMSDataFormatException(str(e) + " - " + str(lims_row))

    # Mark this row as dirty
    dirty_ids[row_id] = row_number

    return lims_row, new


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
                parsed_value = parse_lims_timestamp(parsed_value)
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
