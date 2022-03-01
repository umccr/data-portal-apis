import json

from data_portal.models.workflow import Workflow
from data_processors.pipeline.lambdas import manops
from data_processors.pipeline.orchestration.tests import test_rnasum_step
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class ManOpsLambdaUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_manops.ManOpsLambdaUnitTests.test_handler
        """
        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()

        results = manops.handler(event={
            "event_type": "rnasum",
            "wfr_id": mock_umccrise_workflow.wfr_id,
            "dataset": "KIRC"
        }, context=None)

        submitted_dataset = results['job_list'][0]['dataset']
        self.assertEqual(submitted_dataset, "KIRC")

        logger.info("-" * 32)
        logger.info("Example manops.handler lambda output:")
        logger.info(json.dumps(results))

    def test_handler_not_supported_event(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_manops.ManOpsLambdaUnitTests.test_handler_not_supported_event
        """

        results = manops.handler(event={
            "event_type": "blah",
            "wfr_id": "mock",
            "dataset": "mock"
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example manops.handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_manops.ManOpsLambdaUnitTests.test_rnasum_handler
        """
        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()

        results = manops.rnasum_handler(event={
            "wfr_id": mock_umccrise_workflow.wfr_id,
            "dataset": "BLCA"
        }, context=None)

        submitted_dataset = results['job_list'][0]['dataset']
        self.assertEqual(submitted_dataset, "BLCA")

        logger.info("-" * 32)
        logger.info("Example manops.rnasum_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_handler_wfr_not_exist(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_manops.ManOpsLambdaUnitTests.test_rnasum_handler_wfr_not_exist
        """

        results = manops.rnasum_handler(event={
            "wfr_id": "wfr.not_exist",
            "dataset": "blah"
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example manops.rnasum_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_handler_dataset_null(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_manops.ManOpsLambdaUnitTests.test_rnasum_handler_dataset_null
        """

        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()

        results = manops.rnasum_handler(event={
            "wfr_id": mock_umccrise_workflow.wfr_id,
            "dataset": None
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example manops.rnasum_handler lambda output:")
        logger.info(json.dumps(results))
