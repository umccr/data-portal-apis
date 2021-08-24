import json

from data_portal.models import Workflow
from data_portal.tests import factories
from data_processors.pipeline.domain.workflow import WorkflowRule, WorkflowType, SecondaryAnalysisHelper
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class WorkflowDomainUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(WorkflowDomainUnitTests, self).setUp()

    def test_workflow_rule(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_workflow_rule
        """
        mock_workflow: Workflow = factories.WorkflowFactory()
        mock_workflow.output = json.dumps({"output": "some output"})
        wfl_rule = WorkflowRule(mock_workflow).must_associate_sequence_run().must_have_output()
        self.assertIsNotNone(wfl_rule)

    def test_workflow_rule_no_output(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_workflow_rule_no_output
        """
        mock_workflow: Workflow = factories.WorkflowFactory()
        try:
            _ = WorkflowRule(mock_workflow).must_associate_sequence_run().must_have_output()
        except ValueError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(ValueError)

    def test_workflow_rule_no_sequence_run(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_workflow_rule_no_sequence_run
        """
        mock_workflow: Workflow = Workflow()
        try:
            _ = WorkflowRule(mock_workflow).must_associate_sequence_run().must_have_output()
        except ValueError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(ValueError)

    def test_secondary_analysis_helper(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_workflow.WorkflowDomainUnitTests.test_secondary_analysis_helper
        """
        helper = SecondaryAnalysisHelper(WorkflowType.DRAGEN_WGS_QC)
        logger.info(helper.get_workdir_root())
        logger.info(helper.get_output_root())
        eng_params = helper.get_engine_parameters("SBJ0001")
        logger.info(eng_params)
        self.assertIn("workDirectory", eng_params.keys())
        self.assertIn("outputDirectory", eng_params.keys())
        self.assertIn("SBJ0001", eng_params['workDirectory'])
        self.assertIn("SBJ0001", eng_params['outputDirectory'])

        try:
            _ = helper.get_ssm_key_engine_parameters()
        except ValueError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(ValueError)
