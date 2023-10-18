from typing import List

from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils.timezone import now

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, DragenWtsWorkflowFactory, WorkflowFactory, LabMetadataFactory, \
    LibraryRunFactory, TumorNormalWorkflowFactory, TumorLabMetadataFactory, TumorLibraryRunFactory, \
    DragenWgsQcWorkflowFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.services import workflow_srv, libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class WorkflowSrvUnitTests(PipelineUnitTestCase):

    def test_get_succeeded_by_library_id_and_workflow_type(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_succeeded_by_library_id_and_workflow_type
        """

        mock_lbr: LibraryRun = LibraryRun.objects.create(
            library_id=TestConstant.wts_library_id_tumor.value,
            instrument_run_id=TestConstant.instrument_run_id.value,
            run_id=TestConstant.run_id.value,
            lane=TestConstant.wts_lane_tumor_library.value,
            override_cycles=TestConstant.override_cycles.value,
        )

        mock_workflow: Workflow = DragenWtsWorkflowFactory()
        mock_workflow.end = now()
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.output = {}
        mock_workflow.libraryrun_set.add(mock_lbr)
        mock_workflow.save()

        mock_workflow_double: Workflow = Workflow.objects.create(
            type_name=WorkflowType.DRAGEN_WTS.value,
            wfl_id="wfl.double",
            wfr_id="wfr.double",
            wfv_id="wfv.double",
            version="v1",
            input={},
            start="2012-05-28 11:19:42.897000+00:00"
        )
        mock_workflow_double.end = "2012-05-30 11:19:42.897000+00:00"
        mock_workflow_double.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow_double.output = {}
        mock_workflow_double.libraryrun_set.add(mock_lbr)
        mock_workflow_double.save()

        with CaptureQueriesContext(connection) as ctx:
            workflow_list: List[Workflow] = workflow_srv.get_succeeded_by_library_id_and_workflow_type(
                library_id=TestConstant.wts_library_id_tumor.value,
                workflow_type=WorkflowType.DRAGEN_WTS
            )
            # print(ctx.captured_queries)  # uncomment to print SQL

        self.assertEqual(2, len(workflow_list))

        logger.info((workflow_list[0].wfr_id, workflow_list[0].end))
        logger.info((workflow_list[1].wfr_id, workflow_list[1].end))

        latest = workflow_list[0]
        logger.info(f"latest:\t {latest}")
        lbr1 = latest.libraryrun_set.first()
        self.assertEqual(latest.wfr_id, TestConstant.wfr_id.value)
        self.assertEqual(lbr1.library_id, TestConstant.wts_library_id_tumor.value)

    def test_get_succeed_workflows_by_subject_id_and_workflow_type(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_succeed_workflows_by_subject_id_and_workflow_type
        """

        # Test values
        test_subject_id = TestConstant.subject_id.value
        test_wfr_type_name = WorkflowType.UMCCRISE

        # Create Mock datas
        mock_labmetadata = LabMetadataFactory()
        mock_labmetadata.save()
        mock_libraryrun = LibraryRunFactory()
        mock_libraryrun.save()

        mock_workflow: Workflow = WorkflowFactory()
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.type_name = test_wfr_type_name.value
        mock_workflow.wfr_name = f"umccr__automated__umccrise__{test_subject_id}__L2000002__20220222abcdef"
        mock_workflow.save()
        mock_libraryrun.workflows.add(mock_workflow)

        # Test the function
        workflow_list: List[Workflow] = workflow_srv.get_workflows_by_subject_id_and_workflow_type(
            workflow_type=test_wfr_type_name,
            subject_id=test_subject_id)

        # Test result
        self.assertEqual(1, len(workflow_list))
        self.assertTrue(test_subject_id in workflow_list[0].wfr_name)

    def test_get_running_workflows_by_subject_id_and_workflow_type(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_running_workflows_by_subject_id_and_workflow_type
        """

        # Test values
        test_subject_id = TestConstant.subject_id.value
        test_wfr_type_name = WorkflowType.UMCCRISE

        # Create Mock datas
        mock_labmetadata = LabMetadataFactory()
        mock_labmetadata.save()
        mock_libraryrun = LibraryRunFactory()
        mock_libraryrun.save()

        mock_workflow: Workflow = WorkflowFactory()
        mock_workflow.end_status = WorkflowStatus.RUNNING.value
        mock_workflow.type_name = test_wfr_type_name.value
        mock_workflow.wfr_name = f"umccr__automated__umccrise__{test_subject_id}__L2000002__20220222abcdef"
        mock_workflow.save()
        mock_libraryrun.workflows.add(mock_workflow)

        # Test the function
        succeed_workflow_list: List[Workflow] = workflow_srv.get_workflows_by_subject_id_and_workflow_type(
            workflow_type=test_wfr_type_name,
            subject_id=test_subject_id)

        # Test result
        self.assertEqual(0, len(succeed_workflow_list))

        # Test the function
        running_workflow_list: List[Workflow] = workflow_srv.get_workflows_by_subject_id_and_workflow_type(
            workflow_type=test_wfr_type_name, subject_id=test_subject_id, workflow_status=WorkflowStatus.RUNNING)
        # Test result
        self.assertEqual(1, len(running_workflow_list))
        self.assertTrue(test_subject_id in running_workflow_list[0].wfr_name)

    def test_get_labmetadata_from_wfr_id(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_labmetadata_from_wfr_id
        """

        # Test values
        test_subject_id = TestConstant.subject_id.value
        test_wfr_type_name = WorkflowType.UMCCRISE

        # Create Mock datas
        mock_labmetadata = LabMetadataFactory()
        mock_labmetadata.save()
        mock_libraryrun = LibraryRunFactory()
        mock_libraryrun.save()

        mock_workflow: Workflow = WorkflowFactory()
        mock_workflow.end_status = WorkflowStatus.RUNNING.value
        mock_workflow.type_name = test_wfr_type_name.value
        mock_workflow.wfr_name = f"umccr__automated__umccrise__{test_subject_id}__L2000002__20220222abcdef"
        mock_workflow.save()
        mock_libraryrun.workflows.add(mock_workflow)

        # Test the function

        matched_labmetadata: List[LabMetadata] = workflow_srv.get_labmetadata_by_wfr_id(mock_workflow.wfr_id)

        # Test result
        self.assertEqual(1, len(matched_labmetadata))
        self.assertTrue(matched_labmetadata[0].subject_id, test_subject_id)

    def test_get_labmetadata_by_workflow(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_labmetadata_by_workflow
        """

        mock_tumor_normal_workflow: Workflow = TumorNormalWorkflowFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_lbr_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_normal: LibraryRun = LibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_tumor.library_id, mock_lbr_normal.library_id],
            workflow=mock_tumor_normal_workflow,
        )

        meta_list = workflow_srv.get_labmetadata_by_workflow(mock_tumor_normal_workflow)
        self.assertIsNotNone(meta_list)
        logger.info(meta_list)

        self.assertEqual(len(meta_list), 2)
        self.assertIn(mock_meta_wgs_normal, meta_list)

    def test_get_running_by_sequence_run(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_running_by_sequence_run
        """

        mock_wgs_qc_workflow: Workflow = DragenWgsQcWorkflowFactory()

        running: List[Workflow] = workflow_srv.get_running_by_sequence_run(
            sequence_run=mock_wgs_qc_workflow.sequence_run,
            workflow_type=WorkflowType.DRAGEN_WGS_QC
        )

        logger.info(running)
        self.assertEqual(len(running), 1)
        self.assertIsNone(running[0].end)
        self.assertEqual(running[0].end_status, WorkflowStatus.RUNNING.value)

    def test_get_running_by_sequence_run_end_status_null(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_running_by_sequence_run_end_status_null
        """

        mock_wgs_qc_workflow: Workflow = DragenWgsQcWorkflowFactory()
        # Set workflow status to NULL. We should not classify this workflow as 'RUNNING' in this case.
        mock_wgs_qc_workflow.end_status = None
        mock_wgs_qc_workflow.save()

        running: List[Workflow] = workflow_srv.get_running_by_sequence_run(
            sequence_run=mock_wgs_qc_workflow.sequence_run,
            workflow_type=WorkflowType.DRAGEN_WGS_QC
        )

        logger.info(running)
        self.assertEqual(len(running), 0)

    def test_get_succeeded_by_sequence_run(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_succeeded_by_sequence_run
        """

        mock_wgs_qc_workflow: Workflow = DragenWgsQcWorkflowFactory()
        mock_wgs_qc_workflow.end = now()
        mock_wgs_qc_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wgs_qc_workflow.save()

        succeeded: List[Workflow] = workflow_srv.get_succeeded_by_sequence_run(
            sequence_run=mock_wgs_qc_workflow.sequence_run,
            workflow_type=WorkflowType.DRAGEN_WGS_QC
        )

        logger.info(succeeded)
        self.assertEqual(len(succeeded), 1)
        self.assertIsNotNone(succeeded[0].end)
        self.assertEqual(succeeded[0].end_status, WorkflowStatus.SUCCEEDED.value)

    def test_get_succeeded_by_sequence_run_end_status_null(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_workflow_srv.WorkflowSrvUnitTests.test_get_succeeded_by_sequence_run_end_status_null
        """

        mock_wgs_qc_workflow: Workflow = DragenWgsQcWorkflowFactory()
        mock_wgs_qc_workflow.end = now()
        mock_wgs_qc_workflow.end_status = None  # make workflow status NULL
        mock_wgs_qc_workflow.save()

        succeeded: List[Workflow] = workflow_srv.get_succeeded_by_sequence_run(
            sequence_run=mock_wgs_qc_workflow.sequence_run,
            workflow_type=WorkflowType.DRAGEN_WGS_QC
        )

        logger.info(succeeded)
        self.assertEqual(len(succeeded), 0)
