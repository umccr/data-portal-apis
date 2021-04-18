from datetime import datetime

from dateutil.parser import parse
from django.db.models import QuerySet
from django.utils.timezone import make_aware
from mockito import when, unstub

from data_portal.models import S3Object, Report
from data_processors.reports import services
from data_processors.reports.tests.case import ReportUnitTestCase, ReportIntegrationTestCase, logger
from data_processors.reports.tests.test_report_uk import KEY_EXPECTED
from data_processors.s3 import helper


class ReportUnitTests(ReportUnitTestCase):

    def setUp(self) -> None:
        super(ReportUnitTests, self).setUp()

    def tearDown(self) -> None:
        unstub()
        super(ReportUnitTests, self).tearDown()  # parent tearDown should be last

    def test_persist_report(self):
        """export DJANGO_SETTINGS_MODULE=data_portal.settings.local
        python manage.py test data_processors.reports.tests.test_report.ReportUnitTests.test_persist_report

        NOTE:
        If you are seeing this error:
            django.db.utils.NotSupportedError: contains lookup is not supported on this database backend.

        Make sure to run against local setting i.e.
            export DJANGO_SETTINGS_MODULE=data_portal.settings.local

        Reason: JSONField query do require MySQL database
        """
        mock_s3_obj = S3Object(
            key=KEY_EXPECTED,
            bucket="some-bucket",
            size=170,
            last_modified_date=make_aware(datetime.now()),
            e_tag="1870ed1a461dad0af6fd8da246343948"
        )
        mock_s3_obj.save()

        mock_s3_event_record = helper.S3EventRecord(
            event_type=helper.S3EventType.EVENT_OBJECT_CREATED,
            event_time=parse("2021-04-16T05:53:42.841Z"),
            s3_bucket_name=mock_s3_obj.bucket,
            s3_object_meta={
                'versionId': "sGc6i4_SXKDncofB7fvEq9z7xsvW7GVl",
                'size': 170,
                'eTag': mock_s3_obj.e_tag,
                'key': mock_s3_obj.key,
                'sequencer': "006078FC7966A0E163"
            }
        )

        mock_s3_content_bytes = b'[{"sample":"SBJ00001_PRJ000001_L0000001","Probability":0.034,"intercept":-3.364,"del.mh.prop":-0.757,"SNV3":2.571,"SV3":-0.877,"SV5":-1.105,"hrdloh_index":0.096,"SNV8":0.079}]\n'

        when(services.libs3).get_s3_object_to_bytes(...).thenReturn(mock_s3_content_bytes)

        services.persist_report(
            bucket=mock_s3_event_record.s3_bucket_name,
            key=mock_s3_event_record.s3_object_meta['key'],
            event_type=mock_s3_event_record.event_type.value,
        )

        report = Report.objects.get(subject_id="SBJ00001", sample_id="PRJ000001", library_id="L0000001")
        logger.info("-" * 32)
        logger.info(f"Found report from db: {report}")
        logger.info(f"Report data: {report.data}")
        self.assertIsNotNone(report)

        # assert report <1-to-1> s3_object has linked
        self.assertEqual(report.s3_object_id, mock_s3_obj.id)

        # try report filter query on data JSONField where `Probability` is 0.034, should be 1 match
        qs: QuerySet = Report.objects.filter(data__contains=[{"Probability": 0.034}])
        self.assertEqual(1, qs.count())


class ReportIntegrationTests(ReportIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
