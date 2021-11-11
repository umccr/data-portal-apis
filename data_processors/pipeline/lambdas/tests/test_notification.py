from datetime import datetime, timedelta

from django.utils.timezone import make_aware
from mockito import verify

from data_portal.models.batchrun import BatchRun
from data_portal.models.workflow import Workflow
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.libraryrun import LibraryRun
from data_portal.tests.factories import BatchRunFactory, WorkflowFactory
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import notification
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger
from utils import libslack


class NotificationUnitTests(PipelineUnitTestCase):

    def _gen(self, count, end, end_status, workflow_type=WorkflowType.DRAGEN_WGS_QC):
        mock_workflow_list = []
        for cnt in range(1, count+1):
            mock_library_run = LibraryRun()
            mock_library_run.library_id = f"L210000{cnt}"
            mock_library_run.lane = cnt
            mock_library_run.run_id = self.mock_sqr.run_id
            mock_library_run.instrument_run_id = self.mock_sqr.instrument_run_id
            mock_library_run.save()

            mock_workflow = Workflow()
            mock_workflow.sequence_run = self.mock_sqr
            mock_workflow.batch_run = self.mock_batch_run

            mock_workflow.type_name = workflow_type.value
            mock_workflow.version = "1.0.1-8e3c687"
            mock_workflow.wfr_id = f"wfr.{_rand(32)}"

            mock_workflow.start = make_aware(datetime.utcnow() - timedelta(hours=1))
            mock_workflow.end = end
            mock_workflow.end_status = end_status

            mock_workflow.save()

            mock_library_run.workflows.add(mock_workflow)
            mock_library_run.save()

            mock_workflow_list.append(mock_workflow)
        return mock_workflow_list

    def test_notify_workflow_status_batch_completed(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_completed
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

        # all DRAGEN_WGS_QC should be notified
        for g in Workflow.objects.filter(type_name=WorkflowType.DRAGEN_WGS_QC.value).all():
            self.assertTrue(g.notified)

        # batch run running should be reset
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertFalse(br.running)

    def test_notify_workflow_status_batch_completed_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_completed_alt
        """
        mock_workflow_type = WorkflowType.DRAGEN_TSO_CTDNA

        mock_bcl_workflow = WorkflowFactory()
        self.mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        self.mock_batch_run: BatchRun = BatchRunFactory()
        self.mock_batch_run.step = mock_workflow_type.value
        self.mock_batch_run.save()
        self._gen(
            count=5,
            end=make_aware(datetime.utcnow()),
            end_status=WorkflowStatus.SUCCEEDED.value,
            workflow_type=mock_workflow_type,
        )

        resp = notification.handler({'batch_run_id': self.mock_batch_run.id}, None)
        logger.info("-" * 32)
        logger.info(f"Slack resp: {resp}")

        # assertions

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

        # all DRAGEN_WGS_QC should be notified
        for g in Workflow.objects.filter(type_name=mock_workflow_type.name).all():
            self.assertTrue(g.notified)

        # batch run running should be reset
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertFalse(br.running)

    def test_notify_workflow_status_batch_completed_alt2(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_completed_alt2
        """
        mock_workflow_type = WorkflowType.DRAGEN_WTS

        mock_bcl_workflow = WorkflowFactory()
        self.mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        self.mock_batch_run: BatchRun = BatchRunFactory()
        self.mock_batch_run.step = mock_workflow_type.value
        self.mock_batch_run.save()
        self._gen(
            count=5,
            end=make_aware(datetime.utcnow()),
            end_status=WorkflowStatus.SUCCEEDED.value,
            workflow_type=mock_workflow_type,
        )

        resp = notification.handler({'batch_run_id': self.mock_batch_run.id}, None)
        logger.info("-" * 32)
        logger.info(f"Slack resp: {resp}")

        # assertions

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

        # all DRAGEN_WGS_QC should be notified
        for g in Workflow.objects.filter(type_name=mock_workflow_type.value).all():
            self.assertTrue(g.notified)

        # batch run running should be reset
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertFalse(br.running)

    def test_notify_workflow_status_batch_running(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_running
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

        # all DRAGEN_WGS_QC should be notified
        for g in Workflow.objects.filter(type_name=WorkflowType.DRAGEN_WGS_QC.value).all():
            self.assertTrue(g.notified)

        # batch run should be running
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertTrue(br.running)

    def test_notify_workflow_status_batch_skip(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_notification.NotificationUnitTests.test_notify_workflow_status_batch_skip
        """
        mock_bcl_workflow = WorkflowFactory()
        self.mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        self.mock_batch_run: BatchRun = BatchRunFactory()
        self._gen(count=5, end=None, end_status=WorkflowStatus.RUNNING.value)

        mock_succeeded_dragen_wgs_qc = Workflow()
        mock_succeeded_dragen_wgs_qc.sequence_run = self.mock_sqr
        mock_succeeded_dragen_wgs_qc.batch_run = self.mock_batch_run
        mock_succeeded_dragen_wgs_qc.type_name = WorkflowType.DRAGEN_WGS_QC.value
        mock_succeeded_dragen_wgs_qc.version = "1.0.1-8e3c687"
        mock_succeeded_dragen_wgs_qc.wfr_id = f"wfr.{_rand(32)}"
        mock_succeeded_dragen_wgs_qc.start = make_aware(datetime.utcnow() - timedelta(hours=1))
        mock_succeeded_dragen_wgs_qc.end = make_aware(datetime.utcnow())
        mock_succeeded_dragen_wgs_qc.end_status = WorkflowStatus.SUCCEEDED.value
        mock_succeeded_dragen_wgs_qc.save()

        resp = notification.handler({'batch_run_id': self.mock_batch_run.id}, None)
        logger.info("-" * 32)
        logger.info(f"Slack resp: {resp}")

        # assertions

        # 5 RUNNING, 1 SUCCEEDED
        self.assertEqual(1, Workflow.objects.filter(
            type_name=WorkflowType.DRAGEN_WGS_QC.value,
            end_status=WorkflowStatus.SUCCEEDED.value
        ).count())
        self.assertEqual(5, Workflow.objects.filter(
            type_name=WorkflowType.DRAGEN_WGS_QC.value,
            end_status=WorkflowStatus.RUNNING.value
        ).count())

        # should not call to slack webhook
        verify(libslack.http.client.HTTPSConnection, times=0).request(...)

        # all DRAGEN_WGS_QC should NOT be notified
        for g in Workflow.objects.filter(type_name=WorkflowType.DRAGEN_WGS_QC.value).all():
            self.assertFalse(g.notified)

        # batch run should be still running
        br: BatchRun = BatchRun.objects.get(pk=self.mock_batch_run.id)
        self.assertTrue(br.running)


class NotificationIntegrationTests(PipelineIntegrationTestCase):
    pass
