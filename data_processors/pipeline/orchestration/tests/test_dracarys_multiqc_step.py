import json
from typing import List, Dict

from django.utils.timezone import now
from libumccr import libjson
from libumccr.aws import libssm, libsqs
from unittest import skip
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import DragenWgsQcWorkflowFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import dracarys_multiqc_step
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger
from mockito import unstub, mock, when

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
        when(dracarys_multiqc_step).collect_gds_multiqc_json_files(...).thenReturn([{ "mock": "mock"}])
        when(dracarys_multiqc_step).get_presign_url_for_single_file(...).thenReturn("http://127.0.0.1")
        when(libsqs).dispatch_jobs(...).thenReturn("ok")
        results = dracarys_multiqc_step.perform(mock_wgs_qc_workflow)
        self.assertIsNotNone(results['dracarys_multiqc_step'])


class DracarysMultiqcStepIntegrationTests(PipelineIntegrationTestCase):
    @skip
    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepIntegrationTests.test_handler
        """

        mock_wgs_qc_workflow = build_wgs_qc_mock()
        results = dracarys_multiqc_step.perform(mock_wgs_qc_workflow)
        self.assertIsNotNone(results['dracarys_multiqc_step'])
