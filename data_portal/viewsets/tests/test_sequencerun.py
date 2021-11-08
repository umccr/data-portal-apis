from django.test import TestCase

from data_portal.viewsets.tests import _logger
from data_portal.tests.factories import SequenceRunFactory

class SequenceRunViewSetTestCase(TestCase):

    def setUp(self):
        SequenceRunFactory()

    def test_get_api(self):
        """
        python manage.py test data_portal.viewsets.tests.test_sequencerun.SequenceRunViewSetTestCase.test_get_api
        """
        # Get metadata list
        _logger.info('Get sequencerun API')
        response = self.client.get('/sequencerun/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        _logger.info('Check if API return result')
        result_response = response.data['results']
        self.assertGreater(len(result_response), 0, 'A result is expected')

        _logger.info('Check if unique data has a signle entry')
        response = self.client.get('/sequencerun/?msg_attr_action=statuschanged')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for unique data')

        _logger.info('Check Invalid keyword')
        response = self.client.get('/sequencerun/?foo=bar')
        results_response = response.data['results']
        self.assertGreater(len(results_response), 0, 'Records are expected')
