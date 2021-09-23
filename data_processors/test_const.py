import logging
from unittest import TestCase

from data_processors.const import ReportHelper

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mock_key = "cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"


class ReportHelperUnitTests(TestCase):

    def test_extract_format(self):
        """
        python manage.py test data_processors.test_const.ReportHelperUnitTests.test_extract_format
        """
        ext = ReportHelper.extract_format(mock_key)
        logger.info(ext)
        self.assertIsNotNone(ext)
        self.assertEqual(ext, ReportHelper.REPORT_EXTENSIONS[0])

    def test_extract_source(self):
        """
        python manage.py test data_processors.test_const.ReportHelperUnitTests.test_extract_source
        """
        source = ReportHelper.extract_source(mock_key)
        logger.info(source)
        self.assertIsNotNone(source)
        self.assertEqual(source, ReportHelper.REPORT_KEYWORDS[0])

    def test_is_report(self):
        """
        python manage.py test data_processors.test_const.ReportHelperUnitTests.test_is_report
        """
        self.assertTrue(ReportHelper.is_report(mock_key))
        self.assertFalse(ReportHelper.is_report("something.else"))
        self.assertFalse(ReportHelper.is_report(ReportHelper.REPORT_EXTENSIONS[0]))
        self.assertFalse(ReportHelper.is_report(ReportHelper.REPORT_KEYWORDS[0]))
