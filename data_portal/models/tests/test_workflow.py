import logging

from django.test import TestCase
from django.utils.timezone import now

from data_portal.fields import IdHelper
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class WorkflowTestCase(TestCase):

    def setUp(self):
        workflow_1 = Workflow.objects.create(
            type_name="DRAGEN_WGS_QC",
            wfr_id="wfr.1234fd1111111111111111111111",
            start=now(),
            end_status='Succeeded',
            portal_run_id=IdHelper.generate_portal_run_id()
        )

        workflow_2 = Workflow.objects.create(
            type_name="TUMOR_NORMAL",
            wfr_id="wfr.1234fd2222222222222222222222",
            start=now(),
            end_status='Succeeded',
            portal_run_id=IdHelper.generate_portal_run_id()
        )

        workflow_3 = Workflow.objects.create(
            type_name="TUMOR_NORMAL",
            wfr_id="wfr.1234fd3333333333333333333333",
            start=now(),
            end_status='Succeeded',
            portal_run_id=IdHelper.generate_portal_run_id()
        )

        # Add Library Run to workflow
        library_run = LibraryRun.objects.create(
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
        library_run.workflows.add(workflow_2, workflow_3)

    def test_get_all_workflow_by_library_id(self):
        """
        python manage.py test data_portal.models.tests.test_workflow.WorkflowTestCase.test_get_all_workflow_by_library_id
        """
        workflow = Workflow.objects.get_by_keyword(library_id="L2000003")
        logger.info(workflow)
        self.assertGreaterEqual(len(workflow), 1, "At least a single workflow is expected")
