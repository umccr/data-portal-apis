import logging

from django.test import TestCase
from data_portal.models import LibraryRun
from django.core.exceptions import ObjectDoesNotExist

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SequenceTestCase(TestCase):
    def setUp(self):
        logger.info('Create Object data')
        LibraryRun.objects.create(
            library_id="L2000001",
            instrument_run_id="191213_A00000_00000_A000000000",
            run_id="r.AAAAAAAAA",
            lane=2,
            override_cycles="",
            coverage_yield="",
            qc_pass=True,
            qc_status="good",
            valid_for_analysis=True
        )
        LibraryRun.objects.create(
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

    def test_get_sequence(self):
        logger.info("Test get success sequence table")
        get_complete_sequence = LibraryRun.objects.get(library_id='L2000002')
        self.assertEqual(get_complete_sequence.library_id, 'L2000002', 'Correct Library ID fetch is expected')

    def test_get_api(self):
        # Get sequence list
        logger.info('Get sequence API')
        response = self.client.get('/libraryrun/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        logger.info('Check if API return result')
        results_response = response.data['results']
        self.assertGreater(len(results_response), 0, 'At least some result is expected')

        logger.info('Check if unique data has a signle entry')
        response = self.client.get('/libraryrun/?instrument_run_id=191213_A00000_00000_A000000000&library_id=L2000002')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for uniqueness')
