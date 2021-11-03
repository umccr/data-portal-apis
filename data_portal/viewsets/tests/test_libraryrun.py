from django.test import TestCase

from data_portal.models.libraryrun import LibraryRun
from data_portal.viewsets.tests import _logger


class LibraryRunViewSetTestCase(TestCase):

    def setUp(self):
        _ = LibraryRun.objects.create(
            library_id="L2000002",
            instrument_run_id="191213_A00000_00000_A000000000",
            run_id="r.AAAAAAAAA",
            lane=2,
            override_cycles="",
            coverage_yield="",
            qc_pass=True,
            qc_status="poor",
            valid_for_analysis=True
        )

    def test_get_api(self):
        """
        python manage.py test data_portal.viewsets.tests.test_libraryrun.LibraryRunViewSetTestCase.test_get_api
        """
        # Get sequence list
        _logger.info('Get sequence API')
        response = self.client.get('/libraryrun/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        _logger.info('Check if API return result')
        results_response = response.data['results']
        self.assertGreater(len(results_response), 0, 'At least some result is expected')

        _logger.info('Check if unique data has a signle entry')
        response = self.client.get('/libraryrun/?instrument_run_id=191213_A00000_00000_A000000000&library_id=L2000002')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for uniqueness')
