import json
from datetime import datetime
from unittest import skip

from mockito import when, mock

from data_processors.pipeline.domain.somalier import HolmesPipeline
from data_processors.pipeline.lambdas import somalier_check
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger


class SomalierCheckUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_somalier_check.SomalierCheckUnitTests.test_handler
        """
        mock_execution_result = {
            "executionArn": "arn:aws:states:ap-southeast-5:01233456789:execution:SomalierCheckStateMachine:somalier_check_1653558046",
            "stateMachineArn": "arn:aws:states:ap-southeast-5:0123456789:stateMachine:SomalierCheckStateMachine",
            "name": "somalier_check__312c26574d6_L4100001__2_dragen_MDX00001__1653558046",
            "status": "SUCCEEDED",
            "startDate": datetime.now(),
            "stopDate": "2022-05-26T19:40:54.763000+10:00",
            "input": "{\"index\": \"gds://vol/analysis_data/SBJ00001/wgs_alignment_qc/20220312c26574d6/L4100001__2_dragen/MDX00001.bam\"}",
            "inputDetails": {
                "included": True
            },
            "output": "[{\"file\":\"gds://vol/analysis_data/SBJ00001/wgs_alignment_qc/20220312c26574d6/L4100001__2_dragen/MDX00001.bam\",\"relatedness\":1000,\"ibs0\":0,\"ibs2\":55555,\"hom_concordance\":1,\"hets_a\":6666,\"hets_b\":6666,\"hets_ab\":11111,\"shared_hets\":6666,\"hom_alts_a\":5555,\"hom_alts_b\":5555,\"shared_hom_alts\":5555,\"n\":11111,\"x_ibs0\":0,\"x_ibs2\":777}]",
            "outputDetails": {
                "included": True
            }
        }
        mock_holmes_pipeline = mock(HolmesPipeline)
        mock_holmes_pipeline.execution_result = mock_execution_result

        when(HolmesPipeline).check(...).thenReturn(mock_holmes_pipeline)
        when(mock_holmes_pipeline).poll().thenReturn(mock_holmes_pipeline)

        # mockito to intercept Holmes pipeline service discovery and make it found
        when(HolmesPipeline).discover_service_id().thenReturn("mock_holmes_fingerprint_service")
        when(HolmesPipeline).discover_service_attributes().thenReturn({
            "checkStepsArn": "checkStepsArn",
            "extractStepsArn": "extractStepsArn",
        })

        results = somalier_check.handler({
            "index": "gds://vol/fol/MDX00001.bam"
        }, None)

        self.assertIsInstance(results, dict)
        self.assertEqual(json.loads(results['output'])[0]['relatedness'], 1000)
        logger.info(json.dumps(results))  # NOTE this json dumps also mimic the AWS CLI Lambda datetime serde


class SomalierCheckIntegrationTests(PipelineIntegrationTestCase):
    # integration test cases are meant to hit IAP/AWS endpoints
    # comment @skip and run
    # when done, pls commented it back

    @skip
    def test_somalier_check_handler(self):
        """NOTE: this will actually run Holmes pipeline. You can monitor it in Step Function console at
            Step Functions > State machines > SomalierCheckStateMachineXXXX

        python manage.py test data_processors.pipeline.lambdas.tests.test_somalier_check.SomalierCheckIntegrationTests.test_somalier_check_handler
        """

        # Create check event
        event = {
            # Output from wfr.0d3dea278b1c471d8316b9d5a242dd34
            "index": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam"
        }

        results = somalier_check.handler(event, None)
        logger.info(results)

        self.assertIsNotNone(results)
        self.assertIsInstance(results, dict)
