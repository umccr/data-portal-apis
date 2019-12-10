from typing import List, Dict
import boto3
from django.test import TestCase
from moto import mock_s3
import os
from django.utils.timezone import now

from data_portal.models import S3Object, LIMSRow, S3LIMS
from data_processors import lims_rewrite_processor, sqs_s3_event_processor, lims_update_processor


# All columns in a LIMS CSV
lims_csv_columns = [
    'IlluminaID', 'Run', 'Timestamp', 'SubjectID', 'SampleID', 'LibraryID',
    'ExternalSubjectID', 'ExternalSampleID', 'ExternalLibraryID', 'SampleName',
    'ProjectOwner', 'ProjectName', 'Type', 'Assay', 'Phenotype', 'Source',
    'Quality', 'Topup', 'SecondaryAnalysis', 'FASTQ', 'NumberFASTQS', 'Results', 'Trello', 'Notes', 'ToDo'
]


def generate_lims_csv_row_dict(id: str) -> dict:
    """
    Generate LIMS csv row dict
    :param id: id of the row, to make this row distinguishable
    :return: row dict
    """
    row = dict()
    for col in lims_csv_columns:
        if col == 'Run':
            row[col] = '1'
        elif col == 'Timestamp':
            row[col] = '2019-01-01'
        else:
            # Normal columns, just use column name as value + id
            row[col] = col + id
    return row


def csv_row_dict_to_string(row: dict) -> str:
    """
    Convert a row dict to a string
    :param row: row dict
    :return: the parsed row string
    """
    return ','.join(row.values()) + '\n'


@mock_s3
class DataProcessorsTests(TestCase):
    """
    Test cases for data processing (lambda) functions
    todo: break this class down into separate classes for different data processing functions
    """

    def setUp(self) -> None:
        super().setUp()
        # Important for testing, ensuring we are not mutating actual infrastructure
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'

        # Mock LIMS bucket info
        os.environ['LIMS_BUCKET_NAME'] = 'lims-bucket'
        os.environ['LIMS_CSV_OBJECT_KEY'] = 'lims.csv'

        # boto3 bug: https://github.com/spulec/moto/issues/2413
        # Work around below taken from https://github.com/spulec/moto/issues/2413#issuecomment-533716310
        boto3.DEFAULT_SESSION = None

        # Create a mocked LIMS s3 bucket
        s3 = boto3.resource('s3', region_name='us-east-1')
        self.bucket = s3.create_bucket(Bucket=os.environ['LIMS_BUCKET_NAME'])

    def tearDown(self) -> None:
        # Delete the mocked bucket after each bucket finishes
        self.bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': os.environ['LIMS_CSV_OBJECT_KEY']
                    }
                ]
            }
        )
        self.bucket.delete()

    def _save_lims_csv(self, rows: List[Dict[str, str]]) -> None:
        """
        Save a list of row dicts to the csv and put it in the mocked S3 bucket
        :param rows: list of row dicts
        """
        csv_data = ','.join(lims_csv_columns) + '\n'  # Generate header row first

        for row in rows:
            csv_data += csv_row_dict_to_string(row)

        # Create a test csv file in the fake S3 bucket
        self.bucket.put_object(
            Bucket=os.environ['LIMS_BUCKET_NAME'], Key=os.environ['LIMS_CSV_OBJECT_KEY'], Body=csv_data
        )

    def test_lims_rewrite_processor(self) -> None:
        """
        Test whether LIMS rewrite processor can process csv data as expected
        """
        # Create test csv data, such that the row has the info to be linked with the s3 object created below
        subject_id = 'subject_id'
        sample_id = 'sample_id'

        row_1 = generate_lims_csv_row_dict('1')
        row_1['SampleID'] = sample_id
        row_1['SubjectID'] = subject_id

        self._save_lims_csv([row_1])

        # Create the s3 object such that key contains what we need to find for s3-lims association
        s3_object = S3Object(bucket='s3-keys-bucket', key=f'{subject_id}/{sample_id}/test.json', size=0, last_modified_date=now(), e_tag='')
        s3_object.save()

        process_results = lims_rewrite_processor.handler(None, None)

        # We should have added the new row
        self.assertEqual(process_results['lims_row_new_count'], 1)
        # The data should have been saved
        self.assertEqual(LIMSRow.objects.get(illumina_id='IlluminaID1', sample_id=sample_id).results, row_1['Results'])
        # No update is expected
        self.assertEqual(process_results['lims_row_update_count'], 0)
        # We should also have created the association
        self.assertEqual(process_results['association_count'], 0)

    def test_lims_update_processor(self) -> None:
        """
        Test whether LIMS rewrite processor can process csv data as expected
        """
        row_1 = generate_lims_csv_row_dict('1')

        # Create and save test csv data
        self._save_lims_csv([row_1])
        # Let data be processed through so we have some existing data
        lims_update_processor.handler(None, None)

        # Now we want to test changing one row (i.e. changing a column arbitrarily, and adding a new row
        new_results = 'NewResults'
        row_1['Results'] = new_results
        row_2 = generate_lims_csv_row_dict('2')
        self._save_lims_csv([row_1, row_2])

        process_results = lims_update_processor.handler(None, None)

        # We should have added a new row
        self.assertEqual(process_results['lims_row_new_count'], 1)
        # We should also have updated one existing row
        self.assertEqual(process_results['lims_row_update_count'], 1)
        # The actual object should have been updated
        self.assertEqual(LIMSRow.objects.get(illumina_id='IlluminaID1', sample_id='SampleID1').results, new_results)
        # We no association should have been created as we havent saved any S3Object record
        self.assertEqual(process_results['association_count'], 0)

        # Test when we have duplicate row id, task should fail
        row_duplicate = generate_lims_csv_row_dict('3')
        self._save_lims_csv([row_duplicate, row_duplicate])

        process_results = lims_update_processor.handler(None, None)
        self.assertEqual(process_results['lims_row_invalid_count'], 1)

    def test_lims_non_nullable_columns(self) -> None:
        """
        Test we can correctly process non-nullable columns and rollback the whole processing if one row doesn't have
        the required values
        """
        row_1 = generate_lims_csv_row_dict('1')
        row_2 = generate_lims_csv_row_dict('2')

        # Use blank values for all non-nullable fields
        row_1['IlluminaID'] = '-'
        row_1['Run'] = '-'
        row_1['Timestamp'] = '-'
        row_1['SampleID'] = '-'
        row_1['LibraryID'] = '-'

        self._save_lims_csv([row_1, row_2])

        process_results = lims_rewrite_processor.handler(None, None)

        self.assertEqual(LIMSRow.objects.count(), 1)
        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(process_results['lims_row_invalid_count'], 1)

    def test_lims_empty_subject_id(self) -> None:
        """
        Test searching for S3-LIMS association will not break when the LIMS row has empty SubjectID
        """
        row_1 = generate_lims_csv_row_dict('1')

        # Use blank values for all non-nullable fields
        row_1['SubjectID'] = '-'
        self._save_lims_csv([row_1])
        # No exception should be thrown
        lims_rewrite_processor.handler(None, None)

    def test_sqs_s3_event_processor(self) -> None:
        """
        Test whether SQS S3 event processor can process event data as expected
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

        # Create the s3-lims association between the lims row and s3 object (to be deleted)
        s3_lims = S3LIMS(s3_object=s3_object, lims_row=lims_row)
        s3_lims.save()

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
                            "key": f'{lims_row.subject_id}/{lims_row.sample_id}/test.json',
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
        # We should expect the existing association removed as well
        self.assertEqual(results['s3_lims_removed_count'], 1)

        self.assertEqual(results['created_count'], 1)
        # We should expect the new association created as well
        self.assertEqual(results['s3_lims_created_count'], 1)
        self.assertEqual(results['unsupported_count'], 0)
