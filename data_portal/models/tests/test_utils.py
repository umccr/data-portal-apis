import logging

from django.test import TestCase
from django.core.exceptions import FieldError
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.utils import filter_object_by_parameter_keyword

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UtilsTestCase(TestCase):
    def setUp(self):
        logger.info('Create Object data')
        _ = LibraryRun.objects.create(
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

    def test_filter_object_by_parameter_keyword(self):

        # Define full queryset as initial queryset
        qs = LibraryRun.objects.all()

        # Define a valid object
        logger.info('Testing valid object')
        keyword_object = {"library_id": "L2000001"}
        filtered_qs = filter_object_by_parameter_keyword(qs, keyword_object)
        self.assertEqual(len(filtered_qs), 1, "Expected a single value is returned.")
        logger.info('Test Pass')

        # Test with invalid query
        logger.info('Testing with invalid object')
        keyword_object = {"lib_id": "L2000001"}

        # Field error is expected as no non matching field_name
        with self.assertRaises(FieldError):
            filtered_qs = filter_object_by_parameter_keyword(qs, keyword_object)
        logger.info('Test Pass')