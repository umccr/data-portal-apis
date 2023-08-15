import json
from datetime import datetime
from unittest import skip

import boto3
from botocore import stub
from botocore.stub import Stubber
from botocore.client import BaseClient
from libumccr import aws
from mockito import when, mock

from data_processors.pipeline.domain.somalier import HolmesPipeline
from data_processors.pipeline.lambdas import somalier_extract
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

        # we create a valid boto3 client and then stub it out to return a mock response
        client = boto3.client('servicediscovery')
        stubber = Stubber(client)
        stubber.add_response('discover_instances', {
            "Instances": [
                {
                    "Attributes": {
                        "extractStepsArn": "arn:fake:extract"
                    }
                }
            ]
        }, {
                                 "NamespaceName": stub.ANY,
                                 "ServiceName": stub.ANY
                             })
        stubber.activate()

        # return the mocked service discovery client
        when(aws).srv_discovery_client(...).thenReturn(client)

        result = somalier_extract.handler({
            "index": "gds://vol/fol/MDX123456.bam",
            "reference": "hg38.rna"
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
            "index": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam",
            "reference": "hg38.rna"
        }

        results = somalier_extract.handler(event, None)
        logger.info(results)

        self.assertIsNotNone(results)
        self.assertIsInstance(results, dict)
