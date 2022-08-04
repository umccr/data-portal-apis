from typing import List

from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils.timezone import now

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.tests.factories import TestConstant, DragenWtsWorkflowFactory, WorkflowFactory, LabMetadataFactory, \
    LibraryRunFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.services import workflow_srv
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
