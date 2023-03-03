import logging

from django.test import TestCase
from django.utils.timezone import now

from data_portal.models.flowmetrics import FlowMetrics
from data_portal.tests.factories import FlowMetricsFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FlowMetricsTestCase(TestCase):
    def setUp(self):
        FlowMetricsFactory()
        logger.info('Create Object data')

    def test_get_by_keyword(self):
        logger.info("Test get FlowMetrics attribute by name")
        FlowMetrics.objects.get_by_keyword()