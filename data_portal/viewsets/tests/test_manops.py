from django.test import TestCase
from data_portal.viewsets.tests import _logger


class ManOpsViewSetTestCase(TestCase):

    def test_get_api(self):
        """
        python manage.py test data_portal.viewsets.tests.test_manops.ManOpsViewSetTestCase.test_get_api
        """
        # Get metadata list
        _logger.info('Get labmetadata API')
        response = self.client.get('/manops/')
        self.assertEqual(response.status_code, 400, 'Bad Request status response is expected')
