from data_portal.tests import factories
from data_processors.pipeline.lambdas import gds_search
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class GdsSearchLambdaUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(GdsSearchLambdaUnitTests, self).setUp()
        factories.GDSFileFactory()  # this create a mock GDSFile record in db

    def test_search_gds_file_with_token(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_gds_search.GdsSearchLambdaUnitTests.test_search_gds_file_with_token
        """
        logger.info("Test match")
        search_payload = {
            'gds_volume_name': 'umccr-run-data-dev',
            'tokens': ['Runs', 'B00130']
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertIsNotNone(results)
        self.assertIn("B00130", results['files'][0]['path'])

        logger.info("Test non-match")
        search_payload = {
            'gds_volume_name': 'umccr-run-data-dev',
            'tokens': ['Runs', 'FooBar']
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertEqual(0, len(results['files']))

    def test_search_gds_file_with_token_presigned(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_gds_search.GdsSearchLambdaUnitTests.test_search_gds_file_with_token_presigned
        """
        logger.info("Test match")
        search_payload = {
            'gds_volume_name': 'umccr-run-data-dev',
            'tokens': ['Runs', 'B00130'],
            'presigned': True
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertIsNotNone(results)
        self.assertIsNot(results['files'][0]['presigned_url'], "")  # assert presigned url is not empty

    def test_search_gds_file_with_regex(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_gds_search.GdsSearchLambdaUnitTests.test_search_gds_file_with_regex
        """
        logger.info("Test match")
        search_payload = {
            'gds_volume_name': 'umccr-run-data-dev',
            'regex': '.+Runs.+Test.txt'
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertIsNotNone(results)
        self.assertIn("Test.txt", results['files'][0]['path'])

        logger.info("Test non-match")
        search_payload = {
            'gds_volume_name': 'umccr-run-data-dev',
            'tokens': 'foobar'
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertEqual(0, len(results['files']))
