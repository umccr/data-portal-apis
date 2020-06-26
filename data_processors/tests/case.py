import logging
import os

from django.test import TestCase
from mockito import mock, when, unstub

from data_portal.tests.factories import TestConstant
from data_processors.lambdas import iap

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class WorkflowCase(TestCase):

    def setUp(self) -> None:
        os.environ['IAP_BASE_URL'] = "http://localhost"
        os.environ['IAP_AUTH_TOKEN'] = "mock"
        os.environ['IAP_WES_WORKFLOW_ID'] = TestConstant.wfl_id.value
        os.environ['IAP_WES_WORKFLOW_VERSION_NAME'] = TestConstant.version.value

        # Comment the following mock to actually send slack message for this test case. i.e.
        # export SLACK_CHANNEL=#arteria-dev
        # python manage.py test data_processors.tests.test_iap.IAPLambdaTests.test_sequence_run_event
        #
        os.environ['SLACK_CHANNEL'] = "#mock"
        mock_response = mock(iap.srv.libslack.http.client.HTTPResponse)
        mock_response.status = 200
        when(iap.srv.libslack.libssm).get_ssm_param(...).thenReturn("mock_webhook_id_123")
        when(iap.srv.libslack.http.client.HTTPSConnection).request(...).thenReturn('ok')
        when(iap.srv.libslack.http.client.HTTPSConnection).getresponse(...).thenReturn(mock_response)

    def tearDown(self) -> None:
        del os.environ['IAP_BASE_URL']
        del os.environ['IAP_AUTH_TOKEN']
        del os.environ['IAP_WES_WORKFLOW_ID']
        del os.environ['IAP_WES_WORKFLOW_VERSION_NAME']
        del os.environ['SLACK_CHANNEL']
        unstub()

    def verify(self):
        logger.info(f"IAP_BASE_URL={os.getenv('IAP_BASE_URL')}")
        logger.info(f"IAP_WES_WORKFLOW_ID={os.getenv('IAP_WES_WORKFLOW_ID')}")
        logger.info(f"IAP_WES_WORKFLOW_VERSION_NAME={os.getenv('IAP_WES_WORKFLOW_VERSION_NAME')}")
        assert os.environ['IAP_BASE_URL'] == "http://localhost"
        assert os.environ['IAP_AUTH_TOKEN'] == "mock"
        assert os.environ['IAP_WES_WORKFLOW_ID'] == TestConstant.wfl_id.value
        assert os.environ['IAP_WES_WORKFLOW_VERSION_NAME'] == TestConstant.version.value
        self.assertEqual(os.environ['IAP_BASE_URL'], "http://localhost")
