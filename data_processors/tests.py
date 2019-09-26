from moto import mock_s3
import boto3
import os
from unittest import TestCase, mock
from django.utils.timezone import now

from data_portal.models import S3Object, LIMSRow
from data_processors import lims_rewrite_processor, sqs_s3_event_processor

@mock_s3
class DataProcessorsTests(TestCase):
    """
    Test cases for data processor lambda functions
    """

    # This test wont work: https://github.com/spulec/moto/issues/1793
    @mock.patch.dict(os.environ, {'LIMS_BUCKET_NAME': 'lims-bucket', 'LIMS_CSV_OBJECT_KEY': 'lims.csv'})
    def test_lims_rewrite_processor(self):
        """
        Test whether LIMS rewrite processor can rewrite LIMSRow objects (and S3LIMS objects) using the csv.
        """
        external_subject_id = 'external_subject_id'
        test_csv_data \
            = f"IlluminaID,Run,Timestamp,SubjectID,SampleID,LibraryID,ExternalSubjectID,ExternalSampleID,ExternalLibraryID,\
            SampleName,ProjectOwner,ProjectName,Type,Assay,Phenotype,Source,\
            Quality,Topup,SecondaryAnalysis,FASTQ,NumberFASTQS,Results,Trello,Notes,ToDo\n\
            IlluminaID,1,2019-01-01,SubjectID,SampleID,LibraryID,{external_subject_id},ExternalSampleID,ExternalLibraryID,\
            SampleName,ProjectOwner,ProjectName,Type,Assay,Phenotype,Source,\
            Quality,Topup,SecondaryAnalysis,FASTQ,NumberFASTQS,Results,Trello,Notes,ToDo"

        s3_object = S3Object(bucket='s3-keys-bucket', key=f'{external_subject_id}/file.json', size=0, last_modified_date=now(), e_tag='')
        s3_object.save()

        # Create a fake bucket for csv
        s3 = boto3.resource('s3', region_name='us-east-1')
        bucket = s3.create_bucket(
            Bucket=os.environ['LIMS_BUCKET_NAME'],
        )

        # Create a test csv file in the fake S3 bucket
        bucket.put_object(Bucket=os.environ['LIMS_BUCKET_NAME'],
                          Key=os.environ['LIMS_CSV_OBJECT_KEY'],
                          Body=test_csv_data)

        results = lims_rewrite_processor.handler(None, None)
        self.assertEqual(results['lims_row_count'], 1)
        self.assertEqual(results['association_count'], 1)

    def test_sqs_s3_event_processor(self):
        """
        Test whether LIMS rewrite processor can rewrite LIMSRow objects (and S3LIMS objects) using the csv.
        """
        # Compose test data
        lims_row = LIMSRow(
                illumina_id='Illumina_ID',
                run=1,
                timestamp=now(),
                subject_id='SubjectID',
                sample_id='SampleID',
                library_id='LibraryID',
                external_subject_id='ExternalSubjectID',
                external_sample_id='ExternalSampleID',
                external_library_id='ExternalLibraryID',
                sample_name='SampleName',
                project_owner='ProjectOwner',
                project_name='ProjectName',
                type='Type',
                assay='Assay',
                phenotype='Phenotype',
                source='Source',
                quality='Quality',
                topup='Topup',
                secondary_analysis='SecondaryAnalysis',
                fastq='FASTQ',
                number_fastqs='NumberFASTQS',
                results='Results',
                trello='Trello',
                notes='Notes',
                todo='ToDo'
        )
        lims_row.save()

        bucket_name = 'some-bucket'
        key_to_delete = 'to-delete.json'

        # Create an S3Object first with the key to be deleted
        s3_object = S3Object(bucket=bucket_name, key=key_to_delete, size=0, last_modified_date=now(), e_tag='')
        s3_object.save()

        s3_event_message = {
            "Records": [
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectRemoved",
                    "s3": {
                        "bucket": {
                            "name": bucket_name,
                        },
                        "object": {
                            "key": key_to_delete,
                            "size": 1,
                            "eTag": "object eTag",
                        }
                    }
                },
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated",
                    "s3": {
                        "bucket": {
                            "name": bucket_name
                        },
                        "object": {
                            "key": '%s.json' % lims_row.sample_name,
                            "size": 1,
                            "eTag": "object eTag",
                        }
                    }
                }
            ]
        }

        sqs_event = {
            "Records": [
                {
                    "body": str(s3_event_message),
                }
            ]
        }

        results = sqs_s3_event_processor.handler(sqs_event, None)
        self.assertEqual(results['removed_count'], 1)
        self.assertEqual(results['created_count'], 1)
        self.assertEqual(results['unsupported_count'], 0)
