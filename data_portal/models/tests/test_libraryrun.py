import logging

from django.test import TestCase
from django.utils.timezone import now

from data_portal.fields import IdHelper
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LibraryRunTestCase(TestCase):

    def setUp(self):
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
        library_run_2 = LibraryRun.objects.create(
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
        library_run_3 = LibraryRun.objects.create(
            library_id="L2000003",
            instrument_run_id="191213_A00000_00000_A000000000",
            run_id="r.AAAAAAAAA",
            lane=2,
            override_cycles="",
            coverage_yield="",
            qc_pass=True,
            qc_status="poor",
            valid_for_analysis=True
        )
        workflow_2 = Workflow.objects.create(
            type_name="TUMOR_NORMAL",
            wfr_id="wfr.1234fd2222222222222222222222",
            start=now(),
            end_status='Failed',
            portal_run_id=IdHelper.generate_portal_run_id()
        )
        workflow_3 = Workflow.objects.create(
            type_name="TUMOR_NORMAL",
            wfr_id="wfr.1234fd3333333333333333333333",
            start=now(),
            end_status='Succeeded',
            portal_run_id=IdHelper.generate_portal_run_id()
        )
        library_run_3.workflows.add(workflow_3, workflow_2)
        library_run_2.workflows.add(workflow_2)

    def test_get_library_run(self):
        """
        python manage.py test data_portal.models.tests.test_libraryrun.LibraryRunTestCase.test_get_library_run
        """
        library_run = LibraryRun.objects.get(library_id="L2000002")
        logger.info(library_run)
        self.assertEqual(library_run.library_id, "L2000002", "Correct Library ID fetch is expected")

    def test_get_library_run_by_keyword(self):
        """
        python manage.py test data_portal.models.tests.test_libraryrun.LibraryRunTestCase.test_get_library_run_by_keyword
        """
        result = LibraryRun.objects.get_by_keyword(library_id="L2000002", workflows__end_status="Failed")
        logger.info(result)
        self.assertEqual(len(result), 1, "At least a single libraryrun is expected")
