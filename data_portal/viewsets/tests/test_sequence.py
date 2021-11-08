from django.test import TestCase

from data_portal.models.sequence import Sequence
from data_portal.viewsets.tests import _logger


class SequenceViewSetTestCase(TestCase):

    def setUp(self):
        Sequence.objects.create(
            instrument_run_id="191213_A00000_00000_A000000000",
            run_id="r.AAAAAA",
            sample_sheet_name="SampleSheet.csv",
            gds_folder_path="/to/gds/folder/path",
            gds_volume_name="gds_name",
            status="Complete",
            start_time="2020-01-01 11:59:13.698105"
        )

    def test_get_api(self):
        """
        python manage.py test data_portal.viewsets.tests.test_sequence.SequenceViewSetTestCase.test_get_api
        """
        # Get sequence list
        _logger.info('Get sequence API')
        response = self.client.get('/sequence/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        _logger.info('Check if API return result')
        result_response = response.data['results']
        self.assertGreater(len(result_response), 0, 'A result is expected')

        _logger.info('Check if unique data has a signle entry')
        response = self.client.get('/sequence/?instrument_run_id=191213_A00000_00000_A000000000&run_id=r.AAAAAA')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for unique data')

        _logger.info('Check if wrong parameter')
        response = self.client.get('/sequence/?lib_id=LBR0001')
        results_response = response.data['results']
        self.assertGreater(len(results_response), 0, 'Results are expected for unrecognized query.')
