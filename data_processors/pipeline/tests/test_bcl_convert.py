from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes
from mockito import when

from data_portal.models import SequenceRun, Workflow
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.constant import WorkflowStatus
from data_processors.pipeline.lambdas import bcl_convert
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class BCLConvertUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_handler
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        workflow_json = bcl_convert.handler({
            'gds_volume_name': mock_sqr.gds_volume_name,
            'gds_folder_path': mock_sqr.gds_folder_path,
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info(workflow_json)

        # assert bcl convert workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_handler_alt
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_wfr: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_wfr.id = TestConstant.wfr_id.value
        mock_wfr.time_started = make_aware(datetime.utcnow())
        mock_wfr.status = WorkflowStatus.RUNNING.value
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfr.workflow_version = workflow_version
        when(libwes.WorkflowVersionsApi).launch_workflow_version(...).thenReturn(mock_wfr)

        workflow_json = bcl_convert.handler({
            'gds_volume_name': mock_sqr.gds_volume_name,
            'gds_folder_path': mock_sqr.gds_folder_path,
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("")
        logger.info("Example bcl_convert.handler lambda output:")
        logger.info(workflow_json)

        # assert bcl convert workflow launch success and save workflow runs in db
        success_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, success_bcl_convert_workflow_runs.count())


class BCLConvertIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
