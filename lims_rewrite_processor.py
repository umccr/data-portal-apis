try:
  import unzip_requirements
except ImportError:
  pass

import io
import os, django
# We need to set up django app first
from datetime import datetime

from django.db.models import Q

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# All other imports should be placed below
import csv
import boto3
import logging
from botocore.response import StreamingBody
from django.db import transaction
from data_portal.models import LIMSRow, S3Object, S3LIMS

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    # Trigger rewrite directly
    rewrite_lims_rows()
    return True


def rewrite_lims_rows():
    """
    Check for LIMS data update and synchronise new data to the db
    """
    client = boto3.client('s3')
    # Note that the body data is lazy loaded
    data_object = client.get_object(
        Bucket=os.environ['LIMS_BUCKET_NAME'],
        Key=os.environ['LIMS_CSV_OBJECT_KEY']
    )

    logger.info('Reading csv data')
    body: StreamingBody = data_object['Body']
    bytes_data = body.read()

    with transaction.atomic(), io.BytesIO(bytes_data) as csv_input:
        csv_reader = csv.DictReader(io.TextIOWrapper(csv_input))

        # Delete all rows (and associations) first
        logger.info("Deleting all existing records")
        LIMSRow.objects.all().delete()

        for row in csv_reader:
            lims_row = LIMSRow(
                illumina_id=row['Illumina_ID'],
                run=int(row['Run']),
                timestamp=datetime.strptime(row['Timestamp'], '%Y-%m-%d'),
                sample_id=row['SampleID'],
                sample_name=row['SampleName'],
                project=row['Project'],
                subject_id=row['SubjectID'],
                type=row['Type'],
                phenotype=row['Phenotype'],
                source=row['Source'],
                quality=row['Quality'],
                secondary_analysis=row['Secondary Analysis'],
                fastq=row['FASTQ'],
                number_fastqs=row['Number FASTQS'],
                results=row['Results'],
                trello=row['Trello'],
                notes=row['Notes'].strip(),
                todo=row['ToDo']
            )

            lims_row.save()

            # Find all matching S3Object objects and create association between them and the lims row
            for s3_object in S3Object.objects.filter(
                Q(key__contains=lims_row.sample_name) | Q(key__contains=lims_row.subject_id)
            ):
                association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
                association.save()

    logger.info('Rewrite complete. Total %d rows, %d associations' % (LIMSRow.objects.count(), S3LIMS.objects.count()))
