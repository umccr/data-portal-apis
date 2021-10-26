import logging

from django.test import TestCase
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.utils import filter_object_by_field_keyword

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UtilsTestCase(TestCase):
    def setUp(self):
        logger.info('Create Object data')
        library_run_1 = LibraryRun.objects.create(
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

    def test_get_and_filter_object_by_keyword(self):
        qs = LibraryRun.objects.all()

        field_list = ["id", "library_id", "instrument_run_id", "run_id", "lane", "override_cycles", "coverage_yield",
                      "qc_pass", "qc_status", "valid_for_analysis"]
        result = filter_object_by_field_keyword(qs, field_list, {"library_id": "L2000001"})
        print(result)

        self.assertEqual(len(result), 1, "Expected a single value is returned.")