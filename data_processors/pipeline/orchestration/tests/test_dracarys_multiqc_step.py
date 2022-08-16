import json
from typing import List, Dict

from django.utils.timezone import now
from libumccr import libjson
from unittest import skip
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import DragenWgsQcWorkflowFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import dracarys_multiqc_step
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

# From from wfr.0d3dea278b1c471d8316b9d5a242dd34
mock_wgs_workflow_id = "wfr.0d3dea278b1c471d8316b9d5a242dd34"
mock_wgs_output = json.dumps({
  "dragen_alignment_output_directory": {
    "basename": "L2100747__2_dragen",
    "class": "Directory",
    "location": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen",
    "nameext": "",
    "nameroot": "L2100747__2_dragen",
    "size": None
  },
  "dragen_bam_out": {
    "basename": "MDX210178.bam",
    "class": "File",
    "http://commonwl.org/cwltool#generation": 0,
    "location": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam",
    "nameext": ".bam",
    "nameroot": "MDX210178",
    "secondaryFiles": [
      {
        "basename": "MDX210178.bam.bai",
        "class": "File",
        "http://commonwl.org/cwltool#generation": 0,
        "location": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam.bai",
        "nameext": ".bai",
        "nameroot": "MDX210178.bam"
      }
    ],
    "size": 85852344861
  },
  "multiqc_output_directory": {
    "basename": "MDX210178_dragen_alignment_multiqc",
    "class": "Directory",
    "location": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/MDX210178_dragen_alignment_multiqc",
    "nameext": "",
    "nameroot": "MDX210178_dragen_alignment_multiqc",
    "size": None
  },
  "output_dir_gds_folder_id": "fol.40ebe209462f4ae98b6808d9fe2bff7d",
  "output_dir_gds_session_id": "ssn.ba4412adcc03418f9f63a4bde00473a4",
  "dracarys_output_directory": {
    "basename": "L2100747__2_dragen_dracarys",
    "class": "Directory",
    "location": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen_dracarys",
    "nameext": "",
    "nameroot": "L2100747__2_dragen_dracarys",
    "size": None
  }
})


def build_wgs_qc_mock():
    mock_wgc_qc_workflow: Workflow = DragenWgsQcWorkflowFactory()
    mock_wgc_qc_workflow.wfr_id = mock_wgs_workflow_id
    mock_wgc_qc_workflow.output = mock_wgs_output
    mock_wgc_qc_workflow.end = now()
    mock_wgc_qc_workflow.end_status = WorkflowStatus.SUCCEEDED.value
    mock_wgc_qc_workflow.save()
    return mock_wgc_qc_workflow


class DracarysMultiqcStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepUnitTests.test_perform
        """
        mock_wgs_qc_workflow = build_wgs_qc_mock()
        # TODO mock some WHENs of libgds here
        results = dracarys_multiqc_step.perform(mock_wgs_qc_workflow)
        self.assertIsNotNone(results)

        #logger.info(f"{json.dumps(results)}")
        #self.assertIn('gds_path', results['dracarys_multiqc_step'][0])
        assert True
#    def test_prepare_dracarys_extract_jobs(self):
#        """
#        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysExtractStepUnitTests.test_prepare_dracarys_extract_jobs
#        """
#        mock_wgs_qc_workflow = build_wgs_qc_mock()
#
#        job_list: List[Dict] = dracarys_multiqc_step.prepare_dracarys_extract_jobs(mock_wgs_qc_workflow)
#        self.assertIsNotNone(job_list)
#
#        for job in job_list:
#            logger.info(f"{libjson.dumps(job)}")  # NOTE libjson is intentional and part of serde test
#            self.assertIn("dragen_bam_out", json.loads(mock_wgs_qc_workflow.output).keys())
#            self.assertEqual(job['gds_path'], json.loads(mock_wgs_qc_workflow.output)['dragen_bam_out']['location'])


class DracarysMultiqcStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    #@skip
    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepIntegrationTests.test_handler
        """

        mock_wgs_qc_workflow = build_wgs_qc_mock()
        # TODO mock some WHENs of libgds here
        results = dracarys_multiqc_step.perform(mock_wgs_qc_workflow)
        self.assertIsNotNone(results)

        #logger.info(f"{json.dumps(results)}")
        #self.assertIn('gds_path', results['dracarys_multiqc_step'][0])
        assert True