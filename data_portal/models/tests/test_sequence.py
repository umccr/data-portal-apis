import logging

from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase

from data_portal.models.sequence import Sequence

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
