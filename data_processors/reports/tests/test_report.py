from datetime import datetime

from django.db.models import QuerySet
from django.utils.timezone import make_aware
from libica.app import gds, GDSFilesEventType
from libumccr.aws import libs3
from mockito import when, unstub

from data_portal.models.gdsfile import GDSFile
from data_portal.models.report import Report, ReportType
from data_portal.models.s3object import S3Object
from data_portal.tests.factories import GDSFileFactory
from data_processors.const import ReportHelper
from data_processors.reports.lambdas import report_event
from data_processors.reports.services import s3_report_srv, gds_report_srv
from data_processors.reports.tests.case import ReportUnitTestCase, ReportIntegrationTestCase, logger
from data_processors.reports.tests.test_report_uk import KEY_EXPECTED, KEY_EXPECTED_ALT_7
from data_processors.reports.tests.test_report_uk_gds import PATH_EXPECTED


class ReportUnitTests(ReportUnitTestCase):

    def setUp(self) -> None:
        super(ReportUnitTests, self).setUp()

    def tearDown(self) -> None:
        unstub()
        super(ReportUnitTests, self).tearDown()  # parent tearDown should be last

    def test_report_event_handler(self):
        """export DJANGO_SETTINGS_MODULE=data_portal.settings.local
        python manage.py test data_processors.reports.tests.test_report.ReportUnitTests.test_report_event_handler

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

        mock_s3_content_bytes = b'[{"sample":"SBJ00001_PRJ000001_L0000001","Probability":0.034,"intercept":-3.364,"del.mh.prop":-0.757,"SNV3":2.571,"SV3":-0.877,"SV5":-1.105,"hrdloh_index":0.096,"SNV8":0.079}]\n'

        when(libs3).get_s3_object_to_bytes(...).thenReturn(mock_s3_content_bytes)

        result = report_event.handler({
            'event_type': libs3.S3EventType.EVENT_OBJECT_CREATED.value,
            'event_time': "2021-04-16T05:53:42.841Z",
            's3_bucket_name': mock_s3_obj.bucket,
            's3_object_meta': {
                'versionId': "sGc6i4_SXKDncofB7fvEq9z7xsvW7GVl",
                'size': 170,
                'eTag': mock_s3_obj.e_tag,
                'key': mock_s3_obj.key,
                'sequencer': "006078FC7966A0E163"
            }
        }, None)

        logger.info(result)

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

    def test_report_delete_event(self):
        """
        python manage.py test data_processors.reports.tests.test_report.ReportUnitTests.test_report_delete_event
        """
        report_uri = libs3.get_s3_uri("bucket", KEY_EXPECTED)
        mock_report = Report(
            subject_id="SBJ00001",
            sample_id="PRJ000001",
            library_id="L0000001",
            type=ReportType.HRD_HRDETECT,
            report_uri=report_uri,
            created_by=ReportHelper.REPORT_KEYWORDS[0],
            data={},
            s3_object_id=1,
        )
        mock_report.save()
        logger.info(mock_report)
        self.assertEqual(1, Report.objects.count())

        report = s3_report_srv.persist_report("bucket", KEY_EXPECTED, libs3.S3EventType.EVENT_OBJECT_REMOVED.value)
        self.assertIsNotNone(report)
        self.assertEqual(0, Report.objects.count())

    def test_report_event_handler_gds(self):
        """export DJANGO_SETTINGS_MODULE=data_portal.settings.local
        python manage.py test data_processors.reports.tests.test_report.ReportUnitTests.test_report_event_handler_gds

        NOTE:
        If you are seeing this error:
            django.db.utils.NotSupportedError: contains lookup is not supported on this database backend.

        Make sure to run against local setting i.e.
            export DJANGO_SETTINGS_MODULE=data_portal.settings.local

        Reason: JSONField query do require MySQL database
        """
        mock_gds_file: GDSFile = GDSFileFactory()
        mock_gds_file.path = PATH_EXPECTED
        mock_gds_file.save()

        mock_gds_content_bytes = b'[{"sample":"SBJ00001_PRJ000001_L0000001","Probability":0.034,"intercept":-3.364,"del.mh.prop":-0.757,"SNV3":2.571,"SV3":-0.877,"SV5":-1.105,"hrdloh_index":0.096,"SNV8":0.079}]\n'

        when(gds).get_gds_file_to_bytes(...).thenReturn(mock_gds_content_bytes)

        result = report_event.handler({
            'event_type': GDSFilesEventType.UPLOADED.value,
            'event_time': "2021-04-16T05:53:42.841Z",
            'gds_volume_name': mock_gds_file.volume_name,
            'gds_object_meta': {
                'path': mock_gds_file.path,
                'volumeName': mock_gds_file.volume_name,
            }
        }, None)

        logger.info(result)

        report = Report.objects.get(subject_id="SBJ00001", sample_id="PRJ000001", library_id="L0000001")
        logger.info("-" * 32)
        logger.info(f"Found report from db: {report}")
        logger.info(f"Report data: {report.data}")
        self.assertIsNotNone(report)

        # assert report <1-to-1> gds_file has linked
        self.assertEqual(report.gds_file_id, mock_gds_file.id)

        # try report filter query on data JSONField where `Probability` is 0.034, should be 1 match
        qs: QuerySet = Report.objects.filter(data__contains=[{"Probability": 0.034}])
        self.assertEqual(1, qs.count())

    def test_report_delete_event_gds(self):
        """
        python manage.py test data_processors.reports.tests.test_report.ReportUnitTests.test_report_delete_event_gds
        """
        report_uri = gds.get_gds_uri("volume", PATH_EXPECTED)
        mock_report = Report(
            subject_id="SBJ00001",
            sample_id="PRJ000001",
            library_id="L0000001",
            type=ReportType.FUSION_CALLER_METRICS,
            report_uri=report_uri,
            created_by=ReportHelper.REPORT_KEYWORDS[2],
            data={},
            gds_file_id=1,
        )
        mock_report.save()
        logger.info(mock_report)
        self.assertEqual(1, Report.objects.count())

        report = gds_report_srv.persist_report("volume", PATH_EXPECTED, GDSFilesEventType.DELETED.value)
        self.assertIsNotNone(report)
        self.assertEqual(0, Report.objects.count())

    def test_report_event_handler_multiqc_json_nan(self):
        """export DJANGO_SETTINGS_MODULE=data_portal.settings.local
        python manage.py test data_processors.reports.tests.test_report.ReportUnitTests.test_report_event_handler_multiqc_json_nan

        See https://github.com/umccr/data-portal-apis/issues/347

        NOTE:
        If you are seeing this error:
            django.db.utils.NotSupportedError: contains lookup is not supported on this database backend.

        Make sure to run against local setting i.e.
            export DJANGO_SETTINGS_MODULE=data_portal.settings.local

        Reason: JSONField query do require MySQL database
        """
        mock_s3_obj = S3Object(
            key=KEY_EXPECTED_ALT_7,
            bucket="some-bucket",
            size=170,
            last_modified_date=make_aware(datetime.now()),
            e_tag="1870ed1a461dad0af6fd8da246343948"
        )
        mock_s3_obj.save()

        mock_s3_content_bytes = b'[{"sample":"SBJ00001_PRJ000001_L0000001","data":[0.034, NaN]}]\n'

        when(libs3).get_s3_object_to_bytes(...).thenReturn(mock_s3_content_bytes)

        result = report_event.handler({
            'event_type': libs3.S3EventType.EVENT_OBJECT_CREATED.value,
            'event_time': "2021-04-16T05:53:42.841Z",
            's3_bucket_name': mock_s3_obj.bucket,
            's3_object_meta': {
                'versionId': "sGc6i4_SXKDncofB7fvEq9z7xsvW7GVl",
                'size': 170,
                'eTag': mock_s3_obj.e_tag,
                'key': mock_s3_obj.key,
                'sequencer': "006078FC7966A0E163"
            }
        }, None)

        logger.info(result)

        report = Report.objects.get(subject_id="SBJ00742", sample_id="PRJ210259", library_id="L2100263")
        logger.info("-" * 32)
        logger.info(f"Found report from db: {report}")
        logger.info(f"Report report_uri: {report.report_uri}")
        logger.info(f"Report s3_object_id: {report.s3_object_id}")
        logger.info(f"Report data: {report.data}")
        self.assertIsNone(report.data)

        # assert report <1-to-1> s3_object has linked
        self.assertEqual(report.s3_object_id, mock_s3_obj.id)


class ReportIntegrationTests(ReportIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
