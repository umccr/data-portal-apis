import json

from data_portal.models import Workflow
from data_portal.tests import factories
from data_processors.pipeline.domain.workflow import WorkflowRule
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
