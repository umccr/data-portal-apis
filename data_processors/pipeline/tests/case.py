import logging
import os
import uuid

from django.test import TestCase
from mockito import mock, when, unstub

from data_portal.tests.factories import TestConstant
from utils import libslack, libaws

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PipelineUnitTestCase(TestCase):

    def setUp(self) -> None:
        os.environ['IAP_BASE_URL'] = "http://localhost"
        os.environ['IAP_AUTH_TOKEN'] = "mock"
        os.environ['IAP_WES_WORKFLOW_ID'] = TestConstant.wfl_id.value
        os.environ['IAP_WES_WORKFLOW_VERSION_NAME'] = TestConstant.version.value

        # Comment the following mock to actually send slack message for this test case. i.e.
        # export SLACK_CHANNEL=#arteria-dev
        #
        os.environ['SLACK_CHANNEL'] = "#mock"
        mock_response = mock(libslack.http.client.HTTPResponse)
        mock_response.status = 200
        when(libslack).get_slack_webhook_id_param_store(...).thenReturn("mock_webhook_id_123")
        when(libslack.http.client.HTTPSConnection).request(...).thenReturn('ok')
        when(libslack.http.client.HTTPSConnection).getresponse(...).thenReturn(mock_response)

        mock_sqs = libaws.client(
            'sqs',
            endpoint_url='http://localhost:4566',
            region_name='ap-southeast-2',
            aws_access_key_id=str(uuid.uuid4()),
            aws_secret_access_key=str(uuid.uuid4()),
            aws_session_token=f"{uuid.uuid4()}_{uuid.uuid4()}"
        )
        when(libaws).sqs_client(...).thenReturn(mock_sqs)

    def tearDown(self) -> None:
        del os.environ['IAP_BASE_URL']
        del os.environ['IAP_AUTH_TOKEN']
        del os.environ['IAP_WES_WORKFLOW_ID']
        del os.environ['IAP_WES_WORKFLOW_VERSION_NAME']
        del os.environ['SLACK_CHANNEL']
        unstub()

    def verify_local(self):
        logger.info(f"IAP_BASE_URL={os.getenv('IAP_BASE_URL')}")
        logger.info(f"IAP_WES_WORKFLOW_ID={os.getenv('IAP_WES_WORKFLOW_ID')}")
        logger.info(f"IAP_WES_WORKFLOW_VERSION_NAME={os.getenv('IAP_WES_WORKFLOW_VERSION_NAME')}")
        assert os.environ['IAP_BASE_URL'] == "http://localhost"
        assert os.environ['IAP_AUTH_TOKEN'] == "mock"
        assert os.environ['IAP_WES_WORKFLOW_ID'] == TestConstant.wfl_id.value
        assert os.environ['IAP_WES_WORKFLOW_VERSION_NAME'] == TestConstant.version.value
        self.assertEqual(os.environ['IAP_BASE_URL'], "http://localhost")
        queue_urls = libaws.sqs_client().list_queues()['QueueUrls']
        logger.info(f"SQS_QUEUE_URLS={queue_urls}")
        assert '4566' in queue_urls[0]
        logger.info(f"-" * 32)


class PipelineIntegrationTestCase(TestCase):
    pass
