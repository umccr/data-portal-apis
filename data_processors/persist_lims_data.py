import csv
import io
import logging
import boto3
from botocore.response import StreamingBody
from django.db import transaction, IntegrityError
from django.db.models import Q

from data_portal.models import LIMSRow, S3Object, S3LIMS
from utils.datetime import parse_lims_timestamp

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_lims_data(csv_bucket: str, csv_key: str, rewrite: bool = False):
    """
    Persist lims data into the db
    :param csv_bucket: the s3 bucket storing the csv file
    :param csv_key: the s3 file key of the csv
    :param rewrite: whether we are rewriting the data
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

    with io.BytesIO(bytes_data) as csv_input:
        csv_reader = csv.DictReader(io.TextIOWrapper(csv_input))

    if rewrite:
        # Delete all rows (and associations) first
        logger.info("Deleting all existing records")
        LIMSRow.objects.all().delete()

    for row in csv_reader:
        # todo: pull out new data to to-be-appended list
        lims_row = parse_and_persist_lims_object(row)

        # Find all matching S3Object objects and create association between them and the lims row
        for s3_object in S3Object.objects.filter(Q(key__contains=lims_row.sample_id),
                                                 Q(key__contains=lims_row.subject_id)):
            association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
            association.save()

    lims_row_count = LIMSRow.objects.count()
    association_count = S3LIMS.objects.count()
    logger.info('Rewrite complete. Total %d rows, %d associations' % (lims_row_count, association_count))

    return {
        'lims_row_count': lims_row_count,
        'association_count': association_count,
    }


def parse_and_persist_lims_object(row: dict, force_insert: bool):
    """
    Parse and persist the LIMSRow from the row dict to the db
    :param row: row dict (from csv)
    :param force_insert: whether we are force inserting a row (i.e. don't check for duplicate row identifier)
    :return: saved LIMSRow
    """
    if not force_insert:
        query_set = LIMSRow.objects.filter(illumina_id=row['IlluminaID'], sample_id=row['SampleID'])

        if query_set.exists():
            lims_row = query_set.get()
            # Allow changing any field except the unique identifier
            lims_row.run = int(row['Run']),
            lims_row.timestamp = parse_lims_timestamp(row['Timestamp']),
            lims_row.subject_id = row['SubjectID'],
            lims_row.library_id = row['LibraryID'],
            lims_row.external_subject_id = row['ExternalSubjectID'],
            lims_row.external_sample_id = row['ExternalSampleID'],
            lims_row.external_library_id = row['ExternalLibraryID'],
            lims_row.sample_name = row['SampleName'],
            lims_row.project_owner = row['ProjectOwner'],
            lims_row.project_name = row['ProjectName'],
            lims_row.type = row['Type'],
            lims_row.assay = row['Assay'],
            lims_row.phenotype = row['Phenotype'],
            lims_row.source = row['Source'],
            lims_row.quality = row['Quality'],
            lims_row.topup = row['Topup'],
            lims_row.secondary_analysis = row['SecondaryAnalysis'],
            lims_row.fastq = row['FASTQ'],
            lims_row.number_fastqs = row['NumberFASTQS'],
            lims_row.results = row['Results'],
            lims_row.trello = row['Trello'],
            lims_row.notes = row['Notes'].strip(),
            lims_row.todo = row['ToDo']
        else:
            lims_row = parse_lims_row(row)
    else:
        lims_row = parse_lims_row(row)

    lims_row.save()
    return lims_row


def parse_lims_row(row: dict):
    """
    Parse a LIMSRow from a row dict
    :param row: row dict (from csv)
    :return: parsed LIMSRow object
    """
    return LIMSRow(
        illumina_id=row['IlluminaID'],
        run=int(row['Run']),
        timestamp=parse_lims_timestamp(row['Timestamp']),
        subject_id=row['SubjectID'],
        sample_id=row['SampleID'],
        library_id=row['LibraryID'],
        external_subject_id=row['ExternalSubjectID'],
        external_sample_id=row['ExternalSampleID'],
        external_library_id=row['ExternalLibraryID'],
        sample_name=row['SampleName'],
        project_owner=row['ProjectOwner'],
        project_name=row['ProjectName'],
        type=row['Type'],
        assay=row['Assay'],
        phenotype=row['Phenotype'],
        source=row['Source'],
        quality=row['Quality'],
        topup=row['Topup'],
        secondary_analysis=row['SecondaryAnalysis'],
        fastq=row['FASTQ'],
        number_fastqs=row['NumberFASTQS'],
        results=row['Results'],
        trello=row['Trello'],
        notes=row['Notes'].strip(),
        todo=row['ToDo']
    )
