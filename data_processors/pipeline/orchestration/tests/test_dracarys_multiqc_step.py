import json
from typing import List, Dict

from django.utils.timezone import now
from libumccr import libjson
from libumccr.aws import libssm, libsqs
from libica.app import wes


from unittest import skip
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory
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

    @skip
    def test_handler_wts(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepIntegrationTests.test_handler_wts
        """

        test_wfr="wfr.485aaf32336540ee839a929473708fff"
        mock_workflow: Workflow = WorkflowFactory()

        wesrun = wes.get_run(test_wfr, to_dict=True)
                
        mock_workflow.output = json.dumps(wesrun['output'])
        mock_workflow.input = json.dumps(wesrun['input'])
        mock_workflow.save()

        results = dracarys_multiqc_step.perform(mock_workflow)
        self.assertIsNotNone(results['dracarys_multiqc_step'])

    @skip
    def test_handler_tn(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepIntegrationTests.test_handler_tn
        """

        test_wfr="wfr.25f2c89ce8954d038fdbf268d7cc70c9" # umccr__automated__wgs_tumor_normal__SBJ00716__L2100751__20220312aace133e
        mock_workflow: Workflow = WorkflowFactory()

        wesrun = wes.get_run(test_wfr, to_dict=True)
        
        mock_workflow.output = json.dumps(wesrun['output'])
        mock_workflow.input = json.dumps(wesrun['input'])
        mock_workflow.save()
        
        results = dracarys_multiqc_step.perform(mock_workflow)
        self.assertIsNotNone(results['dracarys_multiqc_step'])

    @skip
    def test_handler_umccrise_nomultiqc(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepIntegrationTests.test_handler_umccrise_nomultiqc
        """
        # there is no multiqc step here, we in fact assume this will return empty
        test_wfr="wfr.ea2419104cbd4d8f837ff93d0d36ba99" # umccr__automated__umccrise__SBJ00915__L2100742__20220313ac60d1d2
        mock_workflow: Workflow = WorkflowFactory()

        wesrun = wes.get_run(test_wfr, to_dict=True)
        
        mock_workflow.output = json.dumps(wesrun['output'])
        mock_workflow.input = json.dumps(wesrun['input'])
        mock_workflow.save()
        
        results = dracarys_multiqc_step.perform(mock_workflow)
        self.assertDictEqual(results,{}) # empty dict apporpiate response

    @skip
    def test_handler_wgs_qc_nomultiqcloc(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dracarys_multiqc_step.DracarysMultiqcStepIntegrationTests.test_handler_wgs_qc_nomultiqcloc
        """
        # there IS multiqc in this output, but it has NO LOCATION - assume an empty response
        test_wfr="wfr.526b30148ada4a8c9fcf0985c6a1170e" # umccr__automated__dragen_wgs_qc__210708_A00130_0166_AH7KTJDSX2__r
        mock_workflow: Workflow = WorkflowFactory()

        wesrun = wes.get_run(test_wfr, to_dict=True)
        
        mock_workflow.output = json.dumps(wesrun['output'])
        mock_workflow.input = json.dumps(wesrun['input'])
        mock_workflow.save()
        
        results = dracarys_multiqc_step.perform(mock_workflow)
        
        self.assertDictEqual(results,{}) # empty dict apporpiate response


        

