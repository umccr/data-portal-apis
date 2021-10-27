import logging

from django.test import TestCase
from data_portal.models.sequence import Sequence
from django.core.exceptions import ObjectDoesNotExist

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SequenceTestCase(TestCase):
    def setUp(self):
        logger.info('Create Object data')
        Sequence.objects.create(
            instrument_run_id="191213_A00000_00000_A000000000",
            run_id="r.AAAAAA",
            sample_sheet_name="SampleSheet.csv",
            gds_folder_path="/to/gds/folder/path",
            gds_volume_name="gds_name",
            status="Complete",
            start_time="2020-01-01 11:59:13.698105"
        )
        Sequence.objects.create(
            instrument_run_id="191213_A00000_00000_A000000000",
            run_id="r.BBBBBB",
            sample_sheet_name="SampleSheet.csv",
            gds_folder_path="/to/gds/folder/path",
            gds_volume_name="gds_name",
            status="Fail",
            start_time="2020-01-01 11:59:13.698105"
        )

    def test_get_sequence(self):

        logger.info("Test get success sequence table")
        get_complete_sequence = Sequence.objects.get(status='Complete')
        self.assertEqual(get_complete_sequence.status, 'Complete', 'Status Complete is expected')

        try:
            Sequence.objects.get(status='Complete')
        except ObjectDoesNotExist:
            logger.info(f"Raised ObjectDoesNotExist")

    def test_get_api(self):

        # Get sequencee list
        logger.info('Get sequence API')
        response = self.client.get('/sequence/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        logger.info('Check if API return result')
        result_response = response.data['results']
        self.assertGreater(len(result_response), 0, 'A result is expected')

        logger.info('Check if unique data has a signle entry')
        response = self.client.get('/sequence/?instrument_run_id=191213_A00000_00000_A000000000&run_id=r.AAAAAA')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for unique data')

        logger.info('Check if wrong parameter')
        response = self.client.get('/sequence/?lib_id=LBR0001')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 0, 'No result is expected for wrong parameter')
