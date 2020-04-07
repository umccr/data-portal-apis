import logging
import os
from typing import List, Dict

import boto3
from django.test import TestCase
from django.utils.timezone import now
from moto import mock_s3

from data_portal.models import S3Object, LIMSRow
from data_processors.lambdas import lims

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
class LIMSLambdaTests(TestCase):
    """
    Test cases for data processing (lambda) functions
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
        s3_object = S3Object(bucket='s3-keys-bucket', key=f'{subject_id}/{sample_id}/test.json', size=0,
                             last_modified_date=now(), e_tag='')
        s3_object.save()

        process_results = lims.rewrite_handler(None, None)

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
        lims.update_handler(None, None)

        # Now we want to test changing one row (i.e. changing a column arbitrarily, and adding a new row
        new_results = 'NewResults'
        row_1['Results'] = new_results
        row_2 = generate_lims_csv_row_dict('2')
        self._save_lims_csv([row_1, row_2])

        process_results = lims.update_handler(None, None)

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

        process_results = lims.update_handler(None, None)
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

        process_results = lims.rewrite_handler(None, None)

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
        lims.rewrite_handler(None, None)
