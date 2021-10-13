from data_portal.models import LibraryRun, Workflow
from data_portal.tests.factories import TumorNormalWorkflowFactory, LibraryRunFactory, TumorLibraryRunFactory, \
    LabMetadataFactory, TumorLabMetadataFactory
from data_processors.pipeline.services import notification_srv, library_run_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase


class NotificationSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(NotificationSrvUnitTests, self).setUp()

    def test_notify_workflow_status(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_notification_srv.NotificationSrvUnitTests.test_notify_workflow_status
        """
        # populate LabMetadata
        mock_normal_metadata = LabMetadataFactory()
        mock_tumor_metadata = TumorLabMetadataFactory()

        # populate LibraryRun
        mock_normal_library_run: LibraryRun = LibraryRunFactory()
        mock_tumor_library_run: LibraryRun = TumorLibraryRunFactory()

        # populate T/N workflow running
        mock_workflow: Workflow = TumorNormalWorkflowFactory()
        mock_workflow.notified = False
        mock_workflow.save()

        # link them
        library_run_srv.link_library_runs_with_x_seq_workflow([
            mock_normal_library_run.library_id,
            mock_tumor_library_run.library_id,
        ], mock_workflow)

        slack_resp = notification_srv.notify_workflow_status(mock_workflow)
        self.assertIsNotNone(slack_resp)

        wfl_in_db = Workflow.objects.get(id=mock_workflow.id)
        self.assertTrue(wfl_in_db.notified)
