import json

from django.utils.timezone import now
from mockito import when

from data_portal.models import Workflow
from data_portal.tests.factories import OncoanalyserWgsWorkflowFactory, TumorNormalWorkflowFactory, \
    TestConstant
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.orchestration import sash_step
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class SashStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_sash_step.SashStepUnitTests.test_perform
        """
        self.verify_local()

        somatic_dir = f"gds://vol1/wgs_tumor_normal/{TestConstant.portal_run_id.value}/Lib1_Lib2_dragen_somatic"
        germline_dir = f"gds://vol1/wgs_tumor_normal/{TestConstant.portal_run_id.value}/Lib1_Lib2_dragen_germline"
        oncoanalyser_dir = f"s3://bk1/analysis_data/SBJ00001/oncoanalyser/{TestConstant.portal_run_id_oncoanalyser.value}/wgs/L2300001__L2300002/SBJ00001_PRJ230001/"

        mock_tn_workflow: Workflow = TumorNormalWorkflowFactory()
        mock_tn_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_tn_workflow.end = now()
        mock_tn_workflow.output = json.dumps({
            "dragen_somatic_output_directory": {
                "location": somatic_dir,
            },
            "dragen_germline_output_directory": {
                "location": germline_dir,
            }
        })
        mock_tn_workflow.save()

        mock_oncoanalyser_wgs: Workflow = OncoanalyserWgsWorkflowFactory()
        mock_oncoanalyser_wgs.output = json.dumps({
            'output_directory': oncoanalyser_dir
        })
        mock_oncoanalyser_wgs.save()

        job = sash_step.perform(mock_oncoanalyser_wgs)

        logger.info(f"{json.dumps(job)}")
        self.assertIsNotNone(job)
        self.assertEqual(job['subject_id'], TestConstant.subject_id.value)
        self.assertEqual(job['tumor_sample_id'], TestConstant.sample_id.value)
        self.assertEqual(job['tumor_library_id'], TestConstant.library_id_tumor.value)
        self.assertEqual(job['dragen_somatic_dir'], somatic_dir)
        self.assertEqual(job['dragen_germline_dir'], germline_dir)
        self.assertEqual(job['oncoanalyser_dir'], oncoanalyser_dir)

    def test_perform_not_my_type(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_sash_step.SashStepUnitTests.test_perform_not_my_type
        """
        mock_oncoanalyser_wgs: Workflow = OncoanalyserWgsWorkflowFactory()
        mock_oncoanalyser_wgs.type_name = WorkflowType.BCL_CONVERT.value
        mock_oncoanalyser_wgs.save()

        job = sash_step.perform(mock_oncoanalyser_wgs)

        logger.info(f"{json.dumps(job)}")
        self.assertEqual(len(job), 0)

    def test_perform_empty_job(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_sash_step.SashStepUnitTests.test_perform_empty_job
        """
        mock_oncoanalyser_wgs: Workflow = OncoanalyserWgsWorkflowFactory()
        when(sash_step).prepare_sash_job(...).thenReturn({})

        job = sash_step.perform(mock_oncoanalyser_wgs)

        logger.info(f"{json.dumps(job)}")
        self.assertEqual(len(job), 0)
