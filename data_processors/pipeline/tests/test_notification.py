from datetime import datetime, timedelta

from django.utils.timezone import make_aware
from mockito import verify

from data_portal.models import BatchRun, Workflow, SequenceRun
from data_portal.tests.factories import BatchRunFactory, WorkflowFactory
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import notification
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger
from utils import libslack


class NotificationUnitTests(PipelineUnitTestCase):

    def _gen(self, count, end, end_status):
        germlines = []
        for cnt in range(1, count+1):
            mock_germline = Workflow()
            mock_germline.sequence_run = self.mock_sqr
            mock_germline.batch_run = self.mock_batch_run

            mock_germline.type_name = WorkflowType.GERMLINE.name
            mock_germline.version = "1.0.1-8e3c687"
            mock_germline.wfr_id = f"wfr.{_rand(32)}"

            mock_germline.sample_name = f"SAMPLE_NAME_{cnt}"
            mock_germline.start = make_aware(datetime.utcnow() - timedelta(hours=1))
            mock_germline.end = end
            mock_germline.end_status = end_status

            mock_germline.save()

            germlines.append(mock_germline)
        return germlines

    def test_notify_workflow_status_batch_completed(self):
        """
        python manage.py test data_processors.pipeline.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_completed
        """
        mock_bcl_workflow = WorkflowFactory()
        self.mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        self.mock_batch_run: BatchRun = BatchRunFactory()
        self._gen(count=50, end=make_aware(datetime.utcnow()), end_status=WorkflowStatus.SUCCEEDED.value)

        resp = notification.handler({'batch_run_id': self.mock_batch_run.id}, None)
        logger.info("-" * 32)
        logger.info(f"Slack resp: {resp}")

        # assertions

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

        # all germline should be notified
        for g in Workflow.objects.filter(type_name=WorkflowType.GERMLINE.name).all():
            self.assertTrue(g.notified)

        # batch run running should be reset
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertFalse(br.running)

    def test_notify_workflow_status_batch_running(self):
        """
        python manage.py test data_processors.pipeline.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_running
        """
        mock_bcl_workflow = WorkflowFactory()
        self.mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        self.mock_batch_run: BatchRun = BatchRunFactory()
        self.mock_batch_run.notified = None
        self.mock_batch_run.save()
        self._gen(count=50, end=None, end_status=WorkflowStatus.RUNNING.value)

        resp = notification.handler({'batch_run_id': self.mock_batch_run.id}, None)
        logger.info("-" * 32)
        logger.info(f"Slack resp: {resp}")

        # assertions

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

        # all germline should be notified
        for g in Workflow.objects.filter(type_name=WorkflowType.GERMLINE.name).all():
            self.assertTrue(g.notified)

        # batch run should be running
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertTrue(br.running)

    def test_notify_workflow_status_batch_skip(self):
        """
        python manage.py test data_processors.pipeline.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_skip
        """
        mock_bcl_workflow = WorkflowFactory()
        self.mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        self.mock_batch_run: BatchRun = BatchRunFactory()
        self._gen(count=5, end=None, end_status=WorkflowStatus.RUNNING.value)

        mock_succeeded_germline = Workflow()
        mock_succeeded_germline.sequence_run = self.mock_sqr
        mock_succeeded_germline.batch_run = self.mock_batch_run
        mock_succeeded_germline.type_name = WorkflowType.GERMLINE.name
        mock_succeeded_germline.version = "1.0.1-8e3c687"
        mock_succeeded_germline.wfr_id = f"wfr.{_rand(32)}"
        mock_succeeded_germline.sample_name = f"SAMPLE_NAME_6"
        mock_succeeded_germline.start = make_aware(datetime.utcnow() - timedelta(hours=1))
        mock_succeeded_germline.end = make_aware(datetime.utcnow())
        mock_succeeded_germline.end_status = WorkflowStatus.SUCCEEDED.value
        mock_succeeded_germline.save()

        resp = notification.handler({'batch_run_id': self.mock_batch_run.id}, None)
        logger.info("-" * 32)
        logger.info(f"Slack resp: {resp}")

        # assertions

        # 5 RUNNING, 1 SUCCEEDED
        self.assertEqual(1, Workflow.objects.filter(
            type_name=WorkflowType.GERMLINE.name,
            end_status=WorkflowStatus.SUCCEEDED.value
        ).count())
        self.assertEqual(5, Workflow.objects.filter(
            type_name=WorkflowType.GERMLINE.name,
            end_status=WorkflowStatus.RUNNING.value
        ).count())

        # should not call to slack webhook
        verify(libslack.http.client.HTTPSConnection, times=0).request(...)

        # all germline should NOT be notified
        for g in Workflow.objects.filter(type_name=WorkflowType.GERMLINE.name).all():
            self.assertFalse(g.notified)

        # batch run should be still running
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertTrue(br.running)


class NotificationIntegrationTests(PipelineIntegrationTestCase):
    pass
