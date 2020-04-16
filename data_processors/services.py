import csv
import io
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Tuple, Dict

from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction
from django.db.models import Q, ExpressionWrapper, Value, CharField, F
from django.utils.dateparse import parse_datetime
from django.utils.timezone import make_aware

from data_portal.models import S3Object, LIMSRow, S3LIMS, GDSFile
from utils import libgdrive, libssm, libs3
from utils.datetime import parse_lims_timestamp

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

    # TODO remove association logic and drop S3LIMS table, related with global search overhaul
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
        logger.debug(f"No association to any LIMS row is found for the S3Object (bucket={bucket}, key={key})")

    return 1, new_association_count


@transaction.atomic
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
        logger.info(f"Deleted S3Object: s3://{bucket_name}/{key}")
        return 1, s3_lims_count
    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent S3Object (bucket={bucket_name}, key={key}): {str(e)}")
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
            logger.error(f"Failed to tag the S3Object ({key}) with ({str(payload)})")

    else:
        # sound of silence
        pass


@transaction.atomic
def persist_lims_data(csv_bucket: str, csv_key: str, rewrite: bool = False) -> Dict[
    str, int]:
    """
    Persist lims data into the db
    :param csv_bucket: the s3 bucket storing the csv file
    :param csv_key: the s3 file key of the csv
    :param rewrite: whether we are rewriting the data
    :return: result statistics - count of updated, new and invalid LIMS rows
    """
    logger.info("Reading LIMS data from S3 bucket")
    bytes_data = libs3.read_s3_object_body(csv_bucket, csv_key)
    csv_input = BytesIO(bytes_data)
    return __persist_lims_data(csv_input, rewrite)


@transaction.atomic
def persist_lims_data_from_google_drive(account_info_ssm_key: str, file_id_ssm_key: str) -> Dict[str, int]:
    requested_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Reading LIMS data from google drive at {requested_time}")

    bytes_data = libgdrive.download_sheet1_csv(
        account_info=libssm.get_secret(account_info_ssm_key),
        file_id=libssm.get_secret(file_id_ssm_key),
    )
    csv_input = BytesIO(bytes_data)
    return __persist_lims_data(csv_input)


@transaction.atomic
def __persist_lims_data(csv_input: BytesIO, rewrite: bool = False) -> Dict[str, int]:
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


@transaction.atomic
def delete_gds_file(payload: dict):
    """
    Payload data structure is dict response of GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    On UMCCR portal, a unique GDS file means unique together of volume_name and path. See GDSFile model.

    :param payload:
    """
    volume_name = payload['volumeName']
    path = payload['path']

    try:
        gds_file = GDSFile.objects.get(volume_name=volume_name, path=path)
        gds_file.delete()
        logger.info(f"Deleted GDSFile: gds://{volume_name}{path}")
    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent GDSFile (volume={volume_name}, path={path}): {str(e)}")


@transaction.atomic
def create_or_update_gds_file(payload: dict):
    """
    Payload data structure is dict response of GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    On UMCCR portal, a unique GDS file means unique together of volume_name and path. See GDSFile model.

    :param payload:
    """
    volume_name = payload.get('volumeName')
    path = payload.get('path')

    qs = GDSFile.objects.filter(volume_name=volume_name, path=path)
    if not qs.exists():
        logger.info(f"Creating a new GDSFile (volume_name={volume_name}, path={path})")
        gds_file = GDSFile()
    else:
        logger.info(f"Updating existing GDSFile (volume_name={volume_name}, path={path})")
        gds_file: GDSFile = qs.get()

    gds_file.file_id = payload.get('id')
    gds_file.name = payload.get('name')
    gds_file.volume_id = payload.get('volumeId')
    gds_file.volume_name = volume_name
    gds_file.type = payload.get('type', None)
    gds_file.tenant_id = payload.get('tenantId')
    gds_file.sub_tenant_id = payload.get('subTenantId')
    gds_file.path = path
    gds_file.time_created = make_aware(parse_datetime(payload.get('timeCreated')))
    gds_file.created_by = payload.get('createdBy')
    gds_file.time_modified = payload.get('timeModified')
    gds_file.modified_by = payload.get('modifiedBy')
    gds_file.inherited_acl = payload.get('inheritedAcl')
    gds_file.urn = payload.get('urn')
    gds_file.size_in_bytes = payload.get('sizeInBytes')
    gds_file.is_uploaded = payload.get('isUploaded')
    gds_file.archive_status = payload.get('archiveStatus')
    gds_file.time_archived = payload.get('timeArchived', None)
    gds_file.storage_tier = payload.get('storageTier')
    gds_file.presigned_url = payload.get('presignedUrl', None)
    gds_file.save()
