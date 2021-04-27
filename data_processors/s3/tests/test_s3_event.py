import json

from django.core.exceptions import ObjectDoesNotExist
from django.utils.timezone import now

from data_portal.models import LIMSRow, S3Object, S3LIMS
from data_processors.s3.lambdas import s3_event
from data_processors.s3.tests.case import S3EventUnitTestCase, S3EventIntegrationTestCase, logger
from data_processors.s3.tests.test_helper import MOCK_REPORT_EVENT


class S3EventUnitTests(S3EventUnitTestCase):

    def test_handler(self) -> None:
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler

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
                            "key": f"{lims_row.subject_id}/{lims_row.sample_id}/test.json",
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
                    "body": json.dumps(s3_event_message),
                }
            ]
        }

        results = s3_event.handler(sqs_event, None)

        self.assertEqual(results['removed_count'], 1)
        # We should expect the existing association removed as well
        self.assertEqual(results['s3_lims_removed_count'], 1)

        self.assertEqual(results['created_count'], 1)
        # We should expect the new association created as well
        self.assertEqual(results['s3_lims_created_count'], 1)
        self.assertEqual(results['unsupported_count'], 0)

    def test_delete_non_existent_s3_object(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_delete_non_existent_s3_object
        """
        s3_event_message = {
            "Records": [
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectRemoved",
                    "s3": {
                        "bucket": {
                            "name": "test",
                        },
                        "object": {
                            "key": "this/dose/not/exist/in/db.txt",
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
                    "body": json.dumps(s3_event_message),
                }
            ]
        }

        s3_event.handler(sqs_event, None)
        self.assertRaises(ObjectDoesNotExist)

    def test_handler_report_queue(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler_report_queue
        """
        self.verify_local()
        results = s3_event.handler(MOCK_REPORT_EVENT, None)
        logger.info(json.dumps(results))
        self.assertEqual(results['created_count'], 1)


class S3EventIntegrationTests(S3EventIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
