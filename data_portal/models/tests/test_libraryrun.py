import logging

from django.test import TestCase

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LibraryRunTestCase(TestCase):

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
            start="2020-01-01 11:59:13.698105",
            end_status='Failed'
        )
        workflow_3 = Workflow.objects.create(
            type_name="TUMOR_NORMAL",
            wfr_id="wfr.1234fd3333333333333333333333",
            start="2020-01-01 11:59:13.698105",
            end_status='Succeeded'
        )
        library_run_3.workflows.add(workflow_3, workflow_2)
        library_run_2.workflows.add(workflow_2)

    def test_get_library_run(self):
        logger.info("Test get success library run table")
        get_library_run = LibraryRun.objects.get(library_id='L2000002')
        self.assertEqual(get_library_run.library_id, 'L2000002', 'Correct Library ID fetch is expected')

    def test_get_library_run_by_keyword(self):
        result = LibraryRun.objects.get_by_keyword(keywords={"library_id":"L2000002", "end_status":"Failed"})
        self.assertEqual(len(result), 1, 'At least a single libraryrun is expected')
