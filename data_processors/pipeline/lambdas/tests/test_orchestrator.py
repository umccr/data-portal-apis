import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.openapi import libwes
from libumccr import libjson
from libumccr.aws import libssm
from mockito import when

from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant, SequenceRunFactory
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.orchestration import dragen_wgs_qc_step, google_lims_update_step, \
    dragen_tso_ctdna_step, fastq_update_step, dragen_wts_step
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class OrchestratorUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(OrchestratorUnitTests, self).setUp()

    def test_bcl_convert_workflow_output_not_json(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorUnitTests.test_bcl_convert_workflow_output_not_json

        Should raise:
            [ERROR] JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
            ...
            ...
            json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes: line 3 column 13 (char 23)

        Storing models.Workflow.output into database should always be in JSON format.
        """
        mock_sqr = SequenceRunFactory()

        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.value
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.sequence_run = mock_sqr
        mock_workflow.output = """
        "main/fastq_list_rows": [
            {
              "rgid": "THIS_DOES_NOT_MATTER_AS_ALREADY_MALFORMED_JSON",
            }
        ]
        """
        try:
            orchestrator.next_step(mock_workflow, {'global': [], 'by_run': {}}, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_bcl_convert_output_unknown_format(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorUnitTests.test_bcl_convert_output_unknown_format
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            'main/fastqs': "say, for example, cwl workflow output is some malformed string, oh well :("
        }

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        try:
            orchestrator.handler({
                'wfr_id': TestConstant.wfr_id.value,
                'wfv_id': TestConstant.wfv_id.value,
            }, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(json.JSONDecodeError)

    def test_skip_list_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorUnitTests.test_skip_list_handler
        """
        mock_workflow = WorkflowFactory()
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.output = ""
        mock_workflow.save()

        # stub at after bcl_convert workflow succeeded state
        when(fastq_update_step).perform(...).thenRaise(ValueError("FASTQ_UPDATE_STEP should not be called"))
        when(google_lims_update_step).perform(...).thenRaise(ValueError("GOOGLE_LIMS_UPDATE_STEP should not be called"))
        when(dragen_wgs_qc_step).perform(...).thenRaise(ValueError("DRAGEN_WGS_QC_STEP should not be called"))
        when(dragen_tso_ctdna_step).perform(...).thenRaise(ValueError("DRAGEN_TSO_CTDNA_STEP should not be called"))
        when(dragen_wts_step).perform(...).thenRaise(ValueError("DRAGEN_WTS_STEP should not be called"))
        when(orchestrator).update_step(...).thenRaise(ValueError("UPDATE_STEP should not be called"))

        skiplist = {
            'global': [
                "UPDATE_STEP",
                "FASTQ_UPDATE_STEP",
                "GOOGLE_LIMS_UPDATE_STEP",
                "DRAGEN_WGS_QC_STEP",
                "DRAGEN_TSO_CTDNA_STEP",
                "DRAGEN_WTS_STEP",
            ],
            'by_run': {}
        }

        event = {
            'wfr_id': mock_workflow.wfr_id,
            'wfv_id': mock_workflow.wfv_id,
            'skip': skiplist,
        }

        results = orchestrator.handler(event, None)
        logger.info(results)

        self.assertEqual(len(results), 0)  # should skip all

    def test_skip_list_no_skip(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorUnitTests.test_skip_list_no_skip
        """
        mock_sqr = SequenceRun()
        mock_sqr.instrument_run_id = TestConstant.instrument_run_id.value

        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.value
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.sequence_run = mock_sqr
        mock_workflow.output = ""

        when(fastq_update_step).perform(...).thenReturn("FASTQ_UPDATE_STEP")
        when(google_lims_update_step).perform(...).thenReturn('GOOGLE_LIMS_UPDATE_STEP')
        when(dragen_wgs_qc_step).perform(...).thenReturn('DRAGEN_WGS_QC_STEP')
        when(dragen_tso_ctdna_step).perform(...).thenReturn('DRAGEN_TSO_CTDNA_STEP')
        when(dragen_wts_step).perform(...).thenReturn('DRAGEN_WTS_STEP')

        skiplist = {
            'global': [],
            'by_run': {}
        }

        results = orchestrator.next_step(mock_workflow, skiplist, None)
        logger.info(results)

        self.assertTrue('DRAGEN_WGS_QC_STEP' in results)
        self.assertTrue('DRAGEN_TSO_CTDNA_STEP' in results)
        self.assertTrue('DRAGEN_WTS_STEP' in results)

    def test_skip_list_run_skip(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorUnitTests.test_skip_list_run_skip
        """
        mock_sqr = SequenceRun()
        mock_sqr.instrument_run_id = TestConstant.instrument_run_id.value

        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.value
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.sequence_run = mock_sqr
        mock_workflow.output = ""

        when(fastq_update_step).perform(...).thenReturn("FASTQ_UPDATE_STEP")
        when(google_lims_update_step).perform(...).thenReturn('GOOGLE_LIMS_UPDATE_STEP')
        when(dragen_wgs_qc_step).perform(...).thenReturn('DRAGEN_WGS_QC_STEP')
        when(dragen_tso_ctdna_step).perform(...).thenReturn('DRAGEN_TSO_CTDNA_STEP')
        when(dragen_wts_step).perform(...).thenReturn('DRAGEN_WTS_STEP')

        run_id = TestConstant.instrument_run_id.value
        skiplist = {
            'global': [],
            'by_run': {
                run_id: [
                    "DRAGEN_WGS_QC_STEP"
                ]
            }
        }

        results = orchestrator.next_step(mock_workflow, skiplist, None)
        logger.info(results)

        self.assertFalse('DRAGEN_WGS_QC_STEP' in results)
        self.assertTrue('DRAGEN_TSO_CTDNA_STEP' in results)
        self.assertTrue('DRAGEN_WTS_STEP' in results)

        skiplist = {
            'global': ["DRAGEN_WGS_QC_STEP"],
            'by_run': {
                run_id: [
                    "DRAGEN_TSO_CTDNA_STEP",
                    "DRAGEN_WTS_STEP"
                ]
            }
        }

        results = orchestrator.next_step(mock_workflow, skiplist, None)
        logger.info(results)

        self.assertFalse('DRAGEN_WGS_QC_STEP' in results)
        self.assertFalse('DRAGEN_TSO_CTDNA_STEP' in results)
        self.assertFalse('DRAGEN_WTS_STEP' in results)

    def test_skip_list_wrong_run_skip(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorUnitTests.test_skip_list_wrong_run_skip
        """
        mock_sqr = SequenceRun()
        mock_sqr.instrument_run_id = TestConstant.instrument_run_id.value

        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.value
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.sequence_run = mock_sqr
        mock_workflow.output = ""

        when(fastq_update_step).perform(...).thenReturn("FASTQ_UPDATE_STEP")
        when(google_lims_update_step).perform(...).thenReturn('GOOGLE_LIMS_UPDATE_STEP')
        when(dragen_wgs_qc_step).perform(...).thenReturn('DRAGEN_WGS_QC_STEP')
        when(dragen_tso_ctdna_step).perform(...).thenReturn('DRAGEN_TSO_CTDNA_STEP')
        when(dragen_wts_step).perform(...).thenReturn('DRAGEN_WTS_STEP')

        run_id = str(TestConstant.instrument_run_id.value).replace("2", "1")
        skiplist = {
            'global': [],
            'by_run': {
                run_id: [
                    "DRAGEN_WGS_QC_STEP"
                ]
            }
        }

        results = orchestrator.next_step(mock_workflow, skiplist, None)
        logger.info(results)

        # by_run skip list should not apply, since run id mismatch, so all workflows should be listed
        self.assertTrue('DRAGEN_WGS_QC_STEP' in results)
        self.assertTrue('DRAGEN_TSO_CTDNA_STEP' in results)
        self.assertTrue('DRAGEN_WTS_STEP' in results)

        skiplist = {
            'global': ["DRAGEN_WGS_QC_STEP"],
            'by_run': {
                run_id: [
                    "DRAGEN_TSO_CTDNA_STEP",
                    "DRAGEN_WTS_STEP"
                ]
            }
        }

        results = orchestrator.next_step(mock_workflow, skiplist, None)
        logger.info(results)

        # only global skip list should apply, due to run ID mismatch
        self.assertFalse('DRAGEN_WGS_QC_STEP' in results)
        self.assertTrue('DRAGEN_TSO_CTDNA_STEP' in results)
        self.assertTrue('DRAGEN_WTS_STEP' in results)


class OrchestratorIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit each test case!
    # and keep decorated @skip after tested

    @skip
    def test_load_skip_list(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_orchestrator.OrchestratorIntegrationTests.test_load_skip_list
        """
        step_skip_list_json = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/step_skip_list")
        step_skip_list = libjson.loads(step_skip_list_json)

        logger.info(step_skip_list)

        self.assertIn('global', step_skip_list)
        self.assertIn("DRAGEN_WTS_STEP", step_skip_list['global'])
        self.assertIn("TUMOR_NORMAL_STEP", step_skip_list['global'])
        self.assertIn("UMCCRISE_STEP", step_skip_list['global'])
