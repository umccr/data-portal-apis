import json
from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes
from mockito import when

from data_portal.models import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.constant import WorkflowStatus
from data_processors.pipeline.lambdas import workflow_update
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class WorkflowUpdateUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_workflow_update.WorkflowUpdateUnitTests.test_handler
        """
        mock_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = mock_workflow.wfr_id
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            'main/fastqs': {
                'location': f"gds://{mock_workflow.wfr_id}/bclConversion_launch/try-1/out-dir-bclConvert",
                'basename': "out-dir-bclConvert",
                'nameroot': "",
                'nameext': "",
                'class': "Directory",
                'listing': []
            }
        }
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        updated_workflow: dict = workflow_update.handler({
            'wfr_id': mock_workflow.wfr_id,
            'wfv_id': mock_workflow.wfv_id,
            'wfr_event': {
                'event_type': "RunSucceeded",
                'event_details': {},
                'timestamp': "2020-06-24T11:27:35.1268588Z"
            }
        }, None)

        logger.info("-"*32)
        logger.info("Example workflow_update.handler lambda output:")
        logger.info(json.dumps(updated_workflow))

        self.assertEqual(updated_workflow['end_status'], WorkflowStatus.SUCCEEDED.value)

    def test_wfr_not_in_db(self):
        """
        python manage.py test data_processors.pipeline.tests.test_workflow_update.WorkflowUpdateUnitTests.test_wfr_not_in_db
        """
        mock_workflow: Workflow = WorkflowFactory()

        updated_workflow: dict = workflow_update.handler({
            'wfr_id': f"wfr.{_rand(32)}",
            'wfv_id': f"wfv.{_rand(32)}",
        }, None)

        self.assertIsNone(updated_workflow)
        self.assertEqual(1, Workflow.objects.all().count())

    def test_notified_status(self):
        """
        python manage.py test data_processors.pipeline.tests.test_workflow_update.WorkflowUpdateUnitTests.test_notified_status
        """
        mock_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = mock_workflow.wfr_id
        mock_wfl_run.status = WorkflowStatus.RUNNING.value
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        updated_workflow: dict = workflow_update.handler({
            'wfr_id': mock_workflow.wfr_id,
            'wfv_id': mock_workflow.wfv_id,
            'wfr_event': {
                'event_type': "RunStarted",
                'event_details': {},
                'timestamp': "2020-06-24T11:27:35.1268588Z"
            }
        }, None)

        logger.info("-"*32)
        logger.info("Example workflow_update.handler lambda output:")
        logger.info(json.dumps(updated_workflow))

        self.assertEqual(updated_workflow['end_status'], WorkflowStatus.RUNNING.value)


class WorkflowUpdateIntegrationTests(PipelineIntegrationTestCase):
    pass
