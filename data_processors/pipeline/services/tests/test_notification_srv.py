from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TumorNormalWorkflowFactory, LibraryRunFactory, TumorLibraryRunFactory, \
    LabMetadataFactory, TumorLabMetadataFactory, WorkflowFactory
from data_processors.pipeline.services import notification_srv, libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


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
        libraryrun_srv.link_library_runs_with_x_seq_workflow([
            mock_normal_library_run.library_id,
            mock_tumor_library_run.library_id,
        ], mock_workflow)

        slack_resp = notification_srv.notify_workflow_status(mock_workflow)
        self.assertIsNotNone(slack_resp)

        wfl_in_db = Workflow.objects.get(id=mock_workflow.id)
        self.assertTrue(wfl_in_db.notified)

    def test_resolve_sample_display_name(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_notification_srv.NotificationSrvUnitTests.test_resolve_sample_display_name
        """
        mock_workflow = WorkflowFactory()

        display_name = notification_srv.resolve_sample_display_name(mock_workflow)
        logger.info(display_name)
        self.assertIsNone(display_name)

    def test_resolve_sample_display_name_bcl_convert(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_notification_srv.NotificationSrvUnitTests.test_resolve_sample_display_name_bcl_convert
        """
        mock_workflow = WorkflowFactory()

        mock_library_run: LibraryRun = LibraryRunFactory()
        mock_library_run.workflows.add(mock_workflow)
        mock_library_run.save()

        display_name = notification_srv.resolve_sample_display_name(mock_workflow)
        logger.info(display_name)
        self.assertIsNone(display_name)

    def test_resolve_sample_display_name_tn(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_notification_srv.NotificationSrvUnitTests.test_resolve_sample_display_name_tn
        """
        mock_workflow = TumorNormalWorkflowFactory()

        mock_meta = TumorLabMetadataFactory()

        mock_library_run: LibraryRun = TumorLibraryRunFactory()
        mock_library_run.workflows.add(mock_workflow)
        mock_library_run.save()

        display_name = notification_srv.resolve_sample_display_name(mock_workflow)
        logger.info(display_name)
        self.assertIsNotNone(display_name)
