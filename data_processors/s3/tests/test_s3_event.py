import json
import logging

from django.core.exceptions import ObjectDoesNotExist
from django.test import override_settings
from django.utils.timezone import now

from data_portal.models.limsrow import LIMSRow, S3LIMS
from data_portal.models.report import Report
from data_portal.models.s3object import S3Object
from data_portal.tests import factories
from data_processors.s3.lambdas import s3_event
from data_processors.s3.tests.case import S3EventUnitTestCase, S3EventIntegrationTestCase, logger

mock_event_no_records = {
    "Records": [
        {
            "messageId": "c260bc61-44e6-400d-8932-29018895a9dpq",
            "receiptHandle": "AQEBVC9HgD6td+WcXqAs9iJELPd1EPUUldyazDnNnPQEvdNwKS/hVgrxw+bYVaZ6xyPo=",
            "body": "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2021-04-18T12:04:10.534Z\",\"Bucket\":\"primary-data-dev\",\"RequestId\":\"AF8W3MW2B6E9YCZ4\",\"HostId\":\"53oAjbUxoZHvQutfDJYELZ2Dxp5M9zX+2++O2g1c3AV5s9cJjf6aTFNAXO42aRa8=\"}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1618747450559",
                "SenderId": "AIDAI32VHCD23ON2HJ2FY",
                "ApproximateFirstReceiveTimestamp": "1618749132911"
            },
            "messageAttributes": {},
            "md5OfBody": "bd152640c0f26ae54042f7c68956f199",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:123456789012:s3-event-queue",
            "awsRegion": "ap-southeast-2"
        }
    ]
}

mock_report_event = {
    "Records": [
        {
            "messageId": "08ffc52c-4c76-40ee-bcdc-94e34432835c",
            "receiptHandle": "AQEBMhLLBmdrQvnw2oWCoEMEXf3//+++//+//+2+/8OAtpIX4wn//+SeqRVSKzaeeynM1uotZc=",
            "body": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"ap-southeast-2\",\"eventTime\":\"2021-04-18T12:35:17.716Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:ABCD4IXYHPYNJHCREIMFQ:john.doe@example.org\"},\"requestParameters\":{\"sourceIPAddress\":\"012.34.56.789\"},\"responseElements\":{\"x-amz-request-id\":\"6TF6RRHQCCC96C6C\",\"x-amz-id-2\":\"cv0vMoMVueNoOoBcQjKM5PDmGgKbDKy3yt0C\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"tf-s3-queue-2021044444444\",\"bucket\":{\"name\":\"primary-data-dev\",\"ownerIdentity\":{\"principalId\":\"A1VCVABCDO40AB\"},\"arn\":\"arn:aws:s3:::primary-data-dev\"},\"object\":{\"key\":\"cancer_report_tables/hrd/SBJ00001__SBJ00001_MDX000001_L0000001_rerun-hrdetect.json.gz\",\"size\":170,\"eTag\":\"1870ed1a461dad0af6fd8da246399999\",\"versionId\":\"La_q0lNQXDZtGvlb_WBf3o6yUVcch4T2\",\"sequencer\":\"12347C27880044A321\"}}}]}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1618749322222",
                "SenderId": "AIDAI32VHCC23ON2AB2AB",
                "ApproximateFirstReceiveTimestamp": "1618749321364"
            },
            "messageAttributes": {},
            "md5OfBody": "3f79cb3e5ef80c45de0c5ea6feac5a30",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:123456789123:s3-event-queue",
            "awsRegion": "ap-southeast-2"
        }
    ]
}


class S3EventUnitTests(S3EventUnitTestCase):

    def test_handler(self) -> None:
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler
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
        results = s3_event.handler(mock_report_event, None)
        logger.info(json.dumps(results))
        self.assertEqual(results['created_count'], 1)

    @override_settings(DEBUG=True)
    def test_delete_s3_object_linked_with_report(self):
        """
        python manage.py test -v 3 data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_delete_s3_object_linked_with_report
        """
        logging.getLogger('django.db.backends').setLevel(logging.DEBUG)

        mock_report: Report = factories.HRDetectReportFactory()
        mock_s3_object: S3Object = factories.ReportLinkedS3ObjectFactory()
        mock_report.s3_object_id = mock_s3_object.id
        mock_report.save()

        s3_event_message = {
            "Records": [
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectRemoved",
                    "s3": {
                        "bucket": {
                            "name": f"{mock_s3_object.bucket}",
                        },
                        "object": {
                            "key": f"{mock_s3_object.key}",
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

        report_in_db = Report.objects.get(id__exact=mock_report.id)
        self.assertIsNotNone(report_in_db.s3_object_id)

    def test_parse_raw_s3_event_records(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_parse_raw_s3_event_records
        """
        event_records_dict = s3_event.parse_raw_s3_event_records(mock_report_event['Records'])
        self.assertEqual(len(event_records_dict['s3_event_records']), 1)
        self.assertEqual(len(event_records_dict['report_event_records']), 1)

    def test_parse_raw_s3_event_records_should_skip(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_parse_raw_s3_event_records_should_skip
        """
        event_records_dict = s3_event.parse_raw_s3_event_records(mock_event_no_records['Records'])
        self.assertEqual(len(event_records_dict['s3_event_records']), 0)
        self.assertEqual(len(event_records_dict['report_event_records']), 0)


class S3EventIntegrationTests(S3EventIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
