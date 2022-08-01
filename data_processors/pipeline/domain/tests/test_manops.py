import logging
from data_portal.tests.factories import  LabMetadataFactory
from data_processors.pipeline.domain.manops import ReportInterface, RNAsumReport
from django.test import TestCase
from data_portal.tests.factories import WorkflowFactory, Workflow, LibraryRunFactory, TestConstant, WorkflowType, \
    WorkflowStatus

logger = logging.getLogger('INFO')


class ReportUnitTests(TestCase):

    def test_abc_report(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_manops.ReportUnitTests.test_abc_report
        """
        try:
            _ = ReportInterface()
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(TypeError)

    def test_rnasum_add_workflow_from_subject(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_manops.ReportUnitTests.test_rnasum_add_workflow_from_subject
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
        report = RNAsumReport()
        report.add_workflow_from_subject("SBJ00001")
        report.add_dataset("BRCA")

        self.assertTrue(report.wfr_id == mock_workflow.wfr_id)
        self.assertTrue(report.dataset == "BRCA")
