import json
from datetime import timedelta, datetime

from django.utils.timezone import make_aware

from data_portal.models import Workflow
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.tests.factories import TestConstant, TumorLabMetadataFactory, TumorLibraryRunFactory, \
    OncoanalyserWtsWorkflowFactory, OncoanalyserWgsWorkflowFactory, \
    LabMetadataFactory, LibraryRunFactory, WtsTumorLabMetadataFactory, WtsTumorLibraryRunFactory, \
    OncoanalyserWtsS3ObjectOutputFactory, OncoanalyserWgsS3ObjectOutputFactory
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.orchestration import oncoanalyser_wgts_existing_both_step
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class OncoanalyserWgtsExistingBothStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_perform
        """
        self.verify_local()

        mock_wts_wfl = OncoanalyserWtsWorkflowFactory()
        mock_wgs_wfl = OncoanalyserWgsWorkflowFactory()

        _ = OncoanalyserWtsS3ObjectOutputFactory()
        _ = OncoanalyserWgsS3ObjectOutputFactory()

        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_lbr_wgs_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_wgs_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wgs_tumor.library_id, mock_lbr_wgs_normal.library_id],
            workflow=mock_wgs_wfl,
        )
        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wts_tumor.library_id],
            workflow=mock_wts_wfl,
        )

        result = oncoanalyser_wgts_existing_both_step.perform(this_workflow=mock_wgs_wfl)

        self.assertIsNotNone(result)
        logger.info(f"{json.dumps(result)}")

        self.assertEqual(result['subject_id'], TestConstant.subject_id.value)
        self.assertEqual(result['normal_wgs_library_id'], mock_meta_wgs_normal.library_id)
        self.assertEqual(result['tumor_wgs_library_id'], mock_meta_wgs_tumor.library_id)
        self.assertEqual(result['tumor_wts_library_id'], mock_meta_wts_tumor.library_id)
        self.assertIn("existing_wgs_dir", result.keys())
        self.assertIn("existing_wts_dir", result.keys())

    def test_perform_no_wts(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_perform_no_wts
        """
        self.verify_local()

        mock_wgs_wfl = OncoanalyserWgsWorkflowFactory()

        _ = OncoanalyserWgsS3ObjectOutputFactory()

        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_lbr_wgs_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_wgs_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wgs_tumor.library_id, mock_lbr_wgs_normal.library_id],
            workflow=mock_wgs_wfl,
        )

        result = oncoanalyser_wgts_existing_both_step.perform(this_workflow=mock_wgs_wfl)

        self.assertIsNotNone(result)
        logger.info(f"{json.dumps(result)}")

        self.assertEqual(result, {})

    def test_perform_no_wgs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_perform_no_wgs
        """
        self.verify_local()

        mock_wts_wfl = OncoanalyserWtsWorkflowFactory()

        _ = OncoanalyserWtsS3ObjectOutputFactory()

        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_lbr_wgs_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_wgs_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wts_tumor.library_id],
            workflow=mock_wts_wfl,
        )

        result = oncoanalyser_wgts_existing_both_step.perform(this_workflow=mock_wts_wfl)

        self.assertIsNotNone(result)
        logger.info(f"{json.dumps(result)}")

        self.assertEqual(result, {})

    def test_perform_wrong_workflow(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_perform_wrong_workflow
        """
        self.verify_local()

        mock_wts_wfl: Workflow = OncoanalyserWtsWorkflowFactory()
        mock_wts_wfl.type_name = WorkflowType.BCL_CONVERT.value
        mock_wts_wfl.save()

        result = oncoanalyser_wgts_existing_both_step.perform(this_workflow=mock_wts_wfl)
        logger.info(result)

        self.assertEqual(result, {})

    def test_prepare_oncoanalyser_wgts_job_wrong_workflow(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_prepare_oncoanalyser_wgts_job_wrong_workflow
        """
        self.verify_local()

        mock_wts_wfl: Workflow = OncoanalyserWtsWorkflowFactory()
        mock_wts_wfl.type_name = WorkflowType.BCL_CONVERT.value
        mock_wts_wfl.save()

        with self.assertRaises(ValueError) as cm:
            oncoanalyser_wgts_existing_both_step.prepare_oncoanalyser_wgts_job(this_workflow=mock_wts_wfl)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Wrong input workflow", str(e))

    def test_find_wts_wf_less_than_1(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_find_wts_wf_less_than_1
        """

        mock_wgs_wfl = OncoanalyserWgsWorkflowFactory()

        result = oncoanalyser_wgts_existing_both_step.find_wts_wf(mock_wgs_wfl)

        logger.info(result)
        self.assertIsNone(result)

    def test_find_wts_wf_more_than_1(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_find_wts_wf_more_than_1
        """

        mock_wts_wfl_older_run = Workflow.objects.create(
            portal_run_id="20230909pppaaaee",
            type_name=WorkflowType.ONCOANALYSER_WTS.value,
            start=make_aware(datetime.now() - timedelta(hours=1, minutes=50)),
            end=make_aware(datetime.now() - timedelta(hours=0, minutes=50)),
            end_status=WorkflowStatus.SUCCEEDED.value,
        )

        mock_wgs_wfl = OncoanalyserWgsWorkflowFactory()
        mock_wts_wfl = OncoanalyserWtsWorkflowFactory()

        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_lbr_wgs_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_wgs_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wgs_tumor.library_id, mock_lbr_wgs_normal.library_id],
            workflow=mock_wgs_wfl,
        )
        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wts_tumor.library_id],
            workflow=mock_wts_wfl,
        )
        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wts_tumor.library_id],
            workflow=mock_wts_wfl_older_run,
        )

        result = oncoanalyser_wgts_existing_both_step.find_wts_wf(mock_wgs_wfl)

        logger.info(result)
        self.assertIsNotNone(result)
        self.assertEqual(result.portal_run_id, TestConstant.portal_run_id2.value)
        self.assertEqual(result.type_name, WorkflowType.ONCOANALYSER_WTS.value)

    def test_find_wgs_wf_less_than_1(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_find_wgs_wf_less_than_1
        """

        mock_wts_wfl = OncoanalyserWtsWorkflowFactory()

        result = oncoanalyser_wgts_existing_both_step.find_wts_wf(mock_wts_wfl)

        logger.info(result)
        self.assertIsNone(result)

    def test_find_wgs_wf_more_than_1(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_find_wgs_wf_more_than_1
        """

        mock_wgs_wfl_older_run = Workflow.objects.create(
            portal_run_id="20230909bbbbaaaee",
            type_name=WorkflowType.ONCOANALYSER_WGS.value,
            start=make_aware(datetime.now() - timedelta(hours=1, minutes=50)),
            end=make_aware(datetime.now() - timedelta(hours=0, minutes=50)),
            end_status=WorkflowStatus.SUCCEEDED.value,
        )

        mock_wgs_wfl = OncoanalyserWgsWorkflowFactory()
        mock_wts_wfl = OncoanalyserWtsWorkflowFactory()

        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_lbr_wgs_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_wgs_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wgs_tumor.library_id, mock_lbr_wgs_normal.library_id],
            workflow=mock_wgs_wfl,
        )
        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wgs_tumor.library_id, mock_lbr_wgs_normal.library_id],
            workflow=mock_wgs_wfl_older_run,
        )
        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wts_tumor.library_id],
            workflow=mock_wts_wfl,
        )

        result = oncoanalyser_wgts_existing_both_step.find_wgs_wf(mock_wts_wfl)

        logger.info(result)
        self.assertIsNotNone(result)
        self.assertEqual(result.portal_run_id, TestConstant.portal_run_id.value)
        self.assertEqual(result.type_name, WorkflowType.ONCOANALYSER_WGS.value)

    def test_get_existing_wgs_dir_raises_error(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_get_existing_wgs_dir_raises_error
        """
        mock_wgs_wfl = OncoanalyserWgsWorkflowFactory()

        with self.assertRaises(ValueError) as cm:
            _ = oncoanalyser_wgts_existing_both_step.get_existing_wgs_dir(mock_wgs_wfl)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Found none", str(e))

    def test_get_existing_wts_dir_raises_error(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_existing_both_step.OncoanalyserWgtsExistingBothStepUnitTests.test_get_existing_wts_dir_raises_error
        """
        mock_wts_wfl = OncoanalyserWtsWorkflowFactory()

        with self.assertRaises(ValueError) as cm:
            _ = oncoanalyser_wgts_existing_both_step.get_existing_wts_dir(mock_wts_wfl)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Found none", str(e))
