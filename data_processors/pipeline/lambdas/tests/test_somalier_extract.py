import json
from datetime import datetime
from unittest import skip

from mockito import when, mock

from data_processors.pipeline.domain.somalier import HolmesPipeline
from data_processors.pipeline.lambdas import somalier_extract, somalier_check
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger


class SomalierExtractUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_somalier_extract.SomalierExtractUnitTests.test_handler
        """

        mock_execution_instance = {
            'executionArn': 'arn:aws:states:ap-southeast-2:0123456789:execution:SomalierExtractStateMachineXXXX:somalier_extract__312c26574d6_L4100007__2_dragen_MDX410001__1653617593',
            'startDate': datetime.now(),
            'ResponseMetadata': {
                'RequestId': '7bc55555-2c7f-4ebf-9829-dcd785a3c283',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amzn-requestid': '7bc55555-2c7f-4ebf-9829-dcd785a3c283',
                    'date': 'Fri, 27 May 2022 02:13:19 GMT',
                    'content-type': 'application/x-amz-json-1.0',
                    'content-length': '220'
                },
                'RetryAttempts': 0
            }
        }

        mock_holmes_pipeline = mock(HolmesPipeline)
        mock_holmes_pipeline.execution_instance = mock_execution_instance
        mock_holmes_pipeline.execution_arn = mock_execution_instance['executionArn']

        when(HolmesPipeline).extract(...).thenReturn(mock_holmes_pipeline)
        when(somalier_check).handler(...).thenReturn(None)

        # mockito to intercept Holmes pipeline service discovery and make it found
        when(HolmesPipeline).discover_service_id().thenReturn("mock_holmes_fingerprint_service")
        when(HolmesPipeline).discover_service_attributes().thenReturn({
            "checkStepsArn": "checkStepsArn",
            "extractStepsArn": "extractStepsArn",
        })

        result = somalier_extract.handler({
            "gds_path": "gds://vol/fol/MDX123456.bam"
        }, None)

        self.assertIsInstance(result, dict)
        self.assertIn('0123456789', result['executionArn'])
        logger.info(json.dumps(result))  # NOTE this json dumps also mimic the AWS CLI Lambda datetime serde


class SomalierExtractIntegrationTests(PipelineIntegrationTestCase):
    # integration test cases are meant to hit IAP/AWS endpoints
    # comment @skip and run
    # when done, pls commented it back

    @skip
    def test_somalier_extract_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_somalier_extract.SomalierExtractIntegrationTests.test_somalier_extract_handler
        """

        # Create extraction event
        event = {
            # Output from wfr.0d3dea278b1c471d8316b9d5a242dd34
            "gds_path": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam"
        }

        results = somalier_extract.handler(event, None)

        self.assertIsNotNone(results)
        self.assertIsInstance(results, dict)
        self.assertIsNotNone(results.get("message", None))
        self.assertEqual(results['message'], "NOT_RUNNING")
        logger.info(results)
