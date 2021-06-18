from data_processors import const
from data_processors.s3 import helper
from data_processors.s3.tests.case import S3EventUnitTestCase, S3EventIntegrationTestCase, logger

_mock_event_no_records = {
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

MOCK_REPORT_EVENT = {
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

MOCK_KEY = "cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"


class S3EventHelperUnitTests(S3EventUnitTestCase):

    def test_parse_raw_s3_event_records(self):
        """
        python manage.py test data_processors.s3.tests.test_helper.S3EventHelperUnitTests.test_parse_raw_s3_event_records
        """
        event_records_dict = helper.parse_raw_s3_event_records(MOCK_REPORT_EVENT['Records'])
        self.assertEqual(len(event_records_dict['s3_event_records']), 1)
        self.assertEqual(len(event_records_dict['report_event_records']), 1)

    def test_parse_raw_s3_event_records_should_skip(self):
        """
        python manage.py test data_processors.s3.tests.test_helper.S3EventHelperUnitTests.test_parse_raw_s3_event_records_should_skip
        """
        event_records_dict = helper.parse_raw_s3_event_records(_mock_event_no_records['Records'])
        self.assertEqual(len(event_records_dict['s3_event_records']), 0)
        self.assertEqual(len(event_records_dict['report_event_records']), 0)

    def test_extract_report_format(self):
        """
        python manage.py test data_processors.s3.tests.test_helper.S3EventHelperUnitTests.test_extract_report_format
        """
        ext = helper.extract_report_format(MOCK_KEY)
        logger.info(ext)
        self.assertIsNotNone(ext)
        self.assertEqual(ext, const.REPORT_EXTENSIONS[0])

    def test_extract_report_source(self):
        """
        python manage.py test data_processors.s3.tests.test_helper.S3EventHelperUnitTests.test_extract_report_source
        """
        source = helper.extract_report_source(MOCK_KEY)
        logger.info(source)
        self.assertIsNotNone(source)
        self.assertEqual(source, const.REPORT_KEYWORDS[0])

    def test_is_report(self):
        """
        python manage.py test data_processors.s3.tests.test_helper.S3EventHelperUnitTests.test_is_report
        """
        self.assertTrue(helper.is_report(MOCK_KEY))
        self.assertFalse(helper.is_report("something.else"))
        self.assertFalse(helper.is_report(const.REPORT_EXTENSIONS[0]))
        self.assertFalse(helper.is_report(const.REPORT_KEYWORDS[0]))


class S3EventHelperIntegrationTests(S3EventIntegrationTestCase):
    pass
