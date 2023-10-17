import json

from libumccr.aws import libssm
from mockito import when, spy2

from data_portal.models.workflow import Workflow
from data_portal.tests import factories
from data_portal.tests.factories import SequenceFactory, TestConstant
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowRule, WorkflowType, SecondaryAnalysisHelper, \
    PrimaryDataHelper, SequenceRule, SequenceRuleError, WorkflowHelper
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class WorkflowDomainUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(WorkflowDomainUnitTests, self).setUp()

    def test_portal_run_id(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_portal_run_id
        """
        helper = WorkflowHelper(WorkflowType.BCL_CONVERT)
        helper2 = WorkflowHelper(WorkflowType.TUMOR_NORMAL)
        logger.info(f"{helper.get_portal_run_id()} != {helper2.get_portal_run_id()}")
        self.assertIsNotNone(helper.portal_run_id)
        self.assertEqual(len(helper.portal_run_id), 16)
        self.assertEqual(helper.get_portal_run_id(), helper.portal_run_id)
        self.assertNotEquals(helper.portal_run_id, helper2.portal_run_id)

    def test_secondary_analysis_helper(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_secondary_analysis_helper
        """
        helper = SecondaryAnalysisHelper(WorkflowType.DRAGEN_WGS_QC)
        logger.info(helper.get_workdir_root())
        logger.info(helper.get_output_root())
        eng_params = helper.get_engine_parameters(target_id="SBJ0001")
        logger.info(eng_params)
        self.assertIn("workDirectory", eng_params)
        self.assertIn("outputDirectory", eng_params)
        self.assertIn("SBJ0001", eng_params['workDirectory'])
        self.assertIn("SBJ0001", eng_params['outputDirectory'])

        tso_helper = SecondaryAnalysisHelper(WorkflowType.DRAGEN_TSO_CTDNA)
        tso_eng_params = tso_helper.get_engine_parameters(target_id="SBJ0002")
        logger.info(tso_eng_params)
        self.assertIn("maxScatter", tso_eng_params)
        self.assertEqual(tso_eng_params['maxScatter'], 8)

    def test_secondary_analysis_helper_block_wgts_qc_type(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_secondary_analysis_helper_block_wgts_qc_type
        """
        with self.assertRaises(ValueError) as cm:
            _ = SecondaryAnalysisHelper(WorkflowType.DRAGEN_WGTS_QC)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Unsupported WorkflowType for Secondary Analysis", str(e))

    def test_secondary_analysis_helper_param_not_found(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_secondary_analysis_helper_param_not_found
        """
        spy2(libssm.get_ssm_param)
        (when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{WorkflowType.DRAGEN_WTS_QC.value}/id")
         .thenRaise(Exception("An error occurred (ParameterNotFound) when calling the GetParameter operation")))

        with self.assertRaises(Exception) as cm:
            _ = SecondaryAnalysisHelper(WorkflowType.DRAGEN_WTS_QC)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("An error occurred (ParameterNotFound)", str(e))

    def test_primary_data_helper(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_primary_data_helper
        """
        helper = PrimaryDataHelper(WorkflowType.BCL_CONVERT)
        logger.info(helper.get_workdir_root())
        logger.info(helper.get_output_root())
        eng_params = helper.get_engine_parameters(target_id="200612_A01052_0017_BH5LYWDSXY")
        logger.info(eng_params)
        self.assertIn("workDirectory", eng_params)
        self.assertIn("outputDirectory", eng_params)
        self.assertIn("200612_A01052_0017_BH5LYWDSXY", eng_params['workDirectory'])
        self.assertIn("200612_A01052_0017_BH5LYWDSXY", eng_params['outputDirectory'])

    def test_primary_data_helper_wrong_wf_type(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_primary_data_helper_wrong_wf_type
        """
        try:
            _ = PrimaryDataHelper(WorkflowType.DRAGEN_WGS_QC)
        except ValueError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(ValueError)


class SequenceDomainRuleUnitTests(PipelineUnitTestCase):

    def test_must_not_emergency_stop(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.SequenceDomainRuleUnitTests.test_must_not_emergency_stop
        """
        mock_sequence = SequenceFactory()
        sr = SequenceRule(mock_sequence).must_not_emergency_stop()
        self.assertIsNotNone(sr)

    def test_must_not_emergency_stop_raise(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.SequenceDomainRuleUnitTests.test_must_not_emergency_stop_raise
        """
        mock_sequence = SequenceFactory()

        when(libssm).get_ssm_param(...).thenReturn(json.dumps([TestConstant.instrument_run_id.value]))

        try:
            sr = SequenceRule(mock_sequence).must_not_emergency_stop()
            self.assertIsNone(sr)
        except SequenceRuleError as se:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{se}")

        self.assertRaises(SequenceRuleError)


class WorkflowDomainRuleUnitTests(PipelineUnitTestCase):

    def test_workflow_rule(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainRuleUnitTests.test_workflow_rule
        """
        mock_workflow: Workflow = factories.WorkflowFactory()
        mock_workflow.output = json.dumps({"output": "some output"})
        wfl_rule = WorkflowRule(mock_workflow).must_associate_sequence_run().must_have_output()
        self.assertIsNotNone(wfl_rule)

    def test_workflow_rule_no_output(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainRuleUnitTests.test_workflow_rule_no_output
        """
        mock_workflow: Workflow = factories.WorkflowFactory()
        try:
            _ = WorkflowRule(mock_workflow).must_associate_sequence_run().must_have_output()
        except ValueError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(ValueError)

    def test_workflow_rule_no_sequence_run(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainRuleUnitTests.test_workflow_rule_no_sequence_run
        """
        mock_workflow: Workflow = Workflow()
        try:
            _ = WorkflowRule(mock_workflow).must_associate_sequence_run().must_have_output()
        except ValueError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(ValueError)


class LabMetadataDomainRuleUnitTests(PipelineUnitTestCase):
    # TODO to complete test all impls from LabMetadataRule
    pass


class LibraryRunDomainRuleUnitTests(PipelineUnitTestCase):
    # TODO to complete test all impls from LibraryRunRule
    pass
