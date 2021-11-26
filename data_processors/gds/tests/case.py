import logging
import os
import uuid

from django.test import TestCase
from libumccr import libslack, aws
from libumccr.aws import libsqs
from mockito import unstub, mock, when

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GDSEventUnitTestCase(TestCase):

    def setUp(self) -> None:
        # Comment the following mock to actually send slack message for this test case. i.e.
        # export SLACK_CHANNEL=#arteria-dev
        #
        os.environ['SLACK_CHANNEL'] = "#mock"
        os.environ['SLACK_WEBHOOK_ID'] = "mock_webhook_id_123"
        mock_response = mock(libslack.http.client.HTTPResponse)
        mock_response.status = 200
        when(libslack.http.client.HTTPSConnection).request(...).thenReturn('ok')
        when(libslack.http.client.HTTPSConnection).getresponse(...).thenReturn(mock_response)

        mock_sqs = aws.client(
            'sqs',
            endpoint_url='http://localhost:4566',
            region_name='ap-southeast-2',
            aws_access_key_id=str(uuid.uuid4()),
            aws_secret_access_key=str(uuid.uuid4()),
            aws_session_token=f"{uuid.uuid4()}_{uuid.uuid4()}"
        )
        when(aws).sqs_client(...).thenReturn(mock_sqs)
        when(libsqs).sqs_client(...).thenReturn(mock_sqs)

    def tearDown(self) -> None:
        del os.environ['SLACK_CHANNEL']
        del os.environ['SLACK_WEBHOOK_ID']
        unstub()

    def verify_local(self):
        queue_urls = libsqs.sqs_client().list_queues()['QueueUrls']
        logger.info(f"SQS_QUEUE_URLS={queue_urls}")
        self.assertIn('4566', queue_urls[0])
        logger.info(f"-" * 32)


class GDSEventIntegrationTestCase(TestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
