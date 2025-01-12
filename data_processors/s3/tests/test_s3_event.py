import json
import logging

from django.core.exceptions import ObjectDoesNotExist
from django.test.utils import override_settings
from django.utils.timezone import now

from data_portal.models import AnalysisResult
from data_portal.models.limsrow import LIMSRow, S3LIMS
from data_portal.models.s3object import S3Object
from data_processors.s3.lambdas import s3_event
from data_processors.s3.tests.case import S3EventUnitTestCase, S3EventIntegrationTestCase, logger

mock_event_no_records = {
    "Records": [
        {
            "messageId": "c260bc61-44e6-400d-8932-29018895a9dpq",
            "receiptHandle": "",
            "body": "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2021-04-18T12:04:10.534Z\",\"Bucket\":\"primary-data-dev\",\"RequestId\":\"AF8W3MW2B6E9YCZ4\",\"HostId\":\"53oAjbUxoZHvQutfDJYELZ2Dxp5M9zX+2++O2g1c3AV5s9cJjf6aTFNAXO42aRa8=\"}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1618747450559",
                "SenderId": "AIDAI32VHCD23ON2HJ2FY",
                "ApproximateFirstReceiveTimestamp": "1618749132911"
            },
            "messageAttributes": {},
            "md5OfBody": "",
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
            "receiptHandle": "",
            "body": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"ap-southeast-2\",\"eventTime\":\"2021-04-18T12:35:17.716Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:ABCD4IXYHPYNJHCREIMFQ:john.doe@example.org\"},\"requestParameters\":{\"sourceIPAddress\":\"012.34.56.789\"},\"responseElements\":{\"x-amz-request-id\":\"6TF6RRHQCCC96C6C\",\"x-amz-id-2\":\"cv0vMoMVueNoOoBcQjKM5PDmGgKbDKy3yt0C\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"tf-s3-queue-2021044444444\",\"bucket\":{\"name\":\"primary-data-dev\",\"ownerIdentity\":{\"principalId\":\"A1VCVABCDO40AB\"},\"arn\":\"arn:aws:s3:::primary-data-dev\"},\"object\":{\"key\":\"cancer_report_tables/hrd/SBJ00001__SBJ00001_MDX000001_L0000001_rerun-hrdetect.json.gz\",\"size\":170,\"eTag\":\"1870ed1a461dad0af6fd8da246399999\",\"versionId\":\"La_q0lNQXDZtGvlb_WBf3o6yUVcch4T2\",\"sequencer\":\"12347C27880044A321\"}}}]}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1618749322222",
                "SenderId": "AIDAI32VHCC23ON2AB2AB",
                "ApproximateFirstReceiveTimestamp": "1618749321364"
            },
            "messageAttributes": {},
            "md5OfBody": "",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:123456789123:s3-event-queue",
            "awsRegion": "ap-southeast-2"
        }
    ]
}

mock_eventbridge_s3_event_object_created = {
    "Records": [
        {
            "messageId": "b18b1ca5-fe74-4b2b-b1db-0063b6ff6b3c",
            "receiptHandle": "AQEBGr...Khw=",
            "body": "{\"version\":\"0\",\"id\":\"93c45b10-e27d-55af-b092-23185cd114d0\",\"detail-type\":\"Object Created\",\"source\":\"aws.s3\",\"account\":\"987654321987\",\"time\":\"2024-06-20T18:49:05Z\",\"region\":\"ap-southeast-2\",\"resources\":[\"arn:aws:s3:::pipeline-dev-cache-987654321987-ap-southeast-2\"],\"detail\":{\"version\":\"0\",\"bucket\":{\"name\":\"pipeline-dev-cache-987654321987-ap-southeast-2\"},\"object\":{\"key\":\"byob-icav2/development/primary/240424_A01052_0193_BH7JMMDRX5/2024062093bae4d5/InterOp/OpticalModelMetricsOut.bin\",\"size\":14363,\"etag\":\"9999999b411d58dc35bf3c978a680647\",\"sequencer\":\"00667479A0F5211022\"},\"request-id\":\"XXXXXXXXJ6KSW1YB\",\"requester\":\"000000148045\",\"source-ip-address\":\"10.153.152.99\",\"reason\":\"CopyObject\"}}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1718909346487",
                "SenderId": "AIDAIDYJ123456T46XWPK",
                "ApproximateFirstReceiveTimestamp": "1718909346489"
            },
            "messageAttributes": {},
            "md5OfBody": "dd195...cb",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:123456789123:s3-event-queue",
            "awsRegion": "ap-southeast-2"
        }
    ]
}

mock_eventbridge_s3_event_object_deleted = {
    "Records": [
        {
            "messageId": "3fb10414-e1d9-4061-85fd-53d58bd32d83",
            "receiptHandle": "AQEBR...3I=",
            "body": "{\"version\":\"0\",\"id\":\"3332f6b6-d092-b2b6-b746-a0473621fbb9\",\"detail-type\":\"Object Deleted\",\"source\":\"aws.s3\",\"account\":\"987654321987\",\"time\":\"2024-06-19T07:51:53Z\",\"region\":\"ap-southeast-2\",\"resources\":[\"arn:aws:s3:::pipeline-dev-cache-987654321987-ap-southeast-2\"],\"detail\":{\"version\":\"0\",\"bucket\":{\"name\":\"pipeline-dev-cache-987654321987-ap-southeast-2\"},\"object\":{\"key\":\"byob-icav2/.iap_upload_test.tmp\",\"sequencer\":\"0066728E192EA8FA5E\"},\"request-id\":\"W40X8C90HKCSB18G\",\"requester\":\"999999321987\",\"source-ip-address\":\"10.153.152.203\",\"reason\":\"DeleteObject\",\"deletion-type\":\"Permanently Deleted\"}}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1718783514561",
                "SenderId": "AIDAIDYJ123456T46XWPK",
                "ApproximateFirstReceiveTimestamp": "1718783514565"
            },
            "messageAttributes": {},
            "md5OfBody": "728eeaf1...0990",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:123456789123:s3-event-queue",
            "awsRegion": "ap-southeast-2"
        }
    ]
}


class S3EventUnitTests(S3EventUnitTestCase):

    @override_settings(DEBUG=True)
    def test_handler(self) -> None:
        """
        python manage.py test -v 3 data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler
        """
        logging.getLogger('django.db.backends').setLevel(logging.DEBUG)

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

        # Create AnalysisResult instance
        analysis_result = AnalysisResult(key='SBJ00001', gen=0, method=0)
        analysis_result.save()

        # Create the s3-analysisresult association
        analysis_result.s3objects.add(s3_object)
        self.assertEqual(AnalysisResult.objects.first().s3objects.count(), 1)  # assert associated

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

        # Simulate second AnalysisResult association. If uncommented, test assertion will fail - which is correct.
        # analysis_result.s3objects.add(S3Object.objects.get(id=2))

        self.assertEqual(results['removed_count'], 1)
        # We should expect the existing association removed as well
        # self.assertEqual(results['s3_lims_removed_count'], 1)     FIXME to be removed when refactoring #343

        self.assertEqual(results['created_count'], 1)
        # We should expect the new association created as well
        # self.assertEqual(results['s3_lims_created_count'], 1)     FIXME to be removed when refactoring #343
        self.assertEqual(results['unsupported_count'], 0)

        # assert AnalysisResult side retain
        self.assertIsNotNone(AnalysisResult.objects.first())
        self.assertEqual(AnalysisResult.objects.count(), 1)
        # assert no more association
        self.assertIsNone(AnalysisResult.objects.get(key='SBJ00001').s3objects.first())
        self.assertEqual(AnalysisResult.objects.first().s3objects.count(), 0)

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

    def test_parse_raw_s3_event_records(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_parse_raw_s3_event_records
        """
        event_records_dict = s3_event.parse_raw_s3_event_records(mock_report_event['Records'])
        self.assertEqual(len(event_records_dict['s3_event_records']), 1)

    def test_parse_raw_s3_event_records_should_skip(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_parse_raw_s3_event_records_should_skip
        """
        event_records_dict = s3_event.parse_raw_s3_event_records(mock_event_no_records['Records'])
        self.assertEqual(len(event_records_dict['s3_event_records']), 0)

    def test_handler_eventbridge_object_created(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler_eventbridge_object_created
        """
        self.verify_local()
        results = s3_event.handler(mock_eventbridge_s3_event_object_created, None)
        logger.info(json.dumps(results))
        self.assertEqual(results['created_count'], 1)

    def test_handler_eventbridge_object_deleted(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler_eventbridge_object_deleted
        """
        self.verify_local()

        _ = S3Object.objects.create(
            bucket='pipeline-dev-cache-987654321987-ap-southeast-2',
            key='byob-icav2/.iap_upload_test.tmp',
            size=123,
            last_modified_date=now(),
            e_tag='',
        )

        results = s3_event.handler(mock_eventbridge_s3_event_object_deleted, None)
        logger.info(json.dumps(results))
        self.assertEqual(results['removed_count'], 1)

    def test_handler_eventbridge_object_deleted_non_existent(self):
        """
        python manage.py test data_processors.s3.tests.test_s3_event.S3EventUnitTests.test_handler_eventbridge_object_deleted_non_existent
        """
        self.verify_local()
        results = s3_event.handler(mock_eventbridge_s3_event_object_deleted, None)
        logger.info(json.dumps(results))
        self.assertEqual(results['removed_count'], 0)


class S3EventIntegrationTests(S3EventIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit each test case!
    # and keep decorated @skip after tested

    pass
