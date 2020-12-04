import logging

from django.test import TestCase
from django.utils.timezone import now

from data_portal.models import Report

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ReportTests(TestCase):

    def test_full_report(self):
        """
        Integration test for sequencing report ingested into database
        """
        bucket = 'unique-hash-bucket'
        key = 'start/umccrise/reports/01_report.json.gz'

        report = Report(1, 2)
        report.save()