import logging
import os
import uuid

from django.test import TestCase
from libumccr import libslack, libgdrive, aws
from libumccr.aws import libsqs
from mockito import mock, when, unstub

from data_portal.tests.factories import TestConstant

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PipelineUnitTestCase(TestCase):

    def setUp(self) -> None:
        os.environ['ICA_BASE_URL'] = "http://localhost"
        os.environ['ICA_ACCESS_TOKEN'] = "mock"
        os.environ['ICA_WES_WORKFLOW_ID'] = TestConstant.wfl_id.value
        os.environ['ICA_WES_WORKFLOW_VERSION_NAME'] = TestConstant.version.value

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

        # At the mo, Google Sheet append update response is ignored. If you like proper response struct then
        # See https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append#response-body
        # See doc string gspread.models.Spreadsheet.values_append()
        mock_sheet_resp = {
            'spreadsheetId': '1vX89Km1D8dm12aTl_552GMVPwOkEHo6sdf1zgI6Rq0g',
            'tableRange': 'Sheet1!A1:AC4557',
            'updates': {
                'spreadsheetId': '1vX89Km1D8dm12aTl_552GMVPwOkEHo6sdf1zgI6Rq0g',
                'updatedRange': 'Sheet1!A4558:AC4558',
                'updatedRows': 1,
                'updatedColumns': 24,
                'updatedCells': 24
            }
        }
        when(libgdrive).append_records(...).thenReturn(mock_sheet_resp)

    def tearDown(self) -> None:
        del os.environ['ICA_BASE_URL']
        del os.environ['ICA_ACCESS_TOKEN']
        del os.environ['ICA_WES_WORKFLOW_ID']
        del os.environ['ICA_WES_WORKFLOW_VERSION_NAME']

        del os.environ['SLACK_CHANNEL']
        del os.environ['SLACK_WEBHOOK_ID']
        unstub()

    def verify_local(self):
        logger.info(f"ICA_BASE_URL={os.getenv('ICA_BASE_URL')}")
        logger.info(f"ICA_WES_WORKFLOW_ID={os.getenv('ICA_WES_WORKFLOW_ID')}")
        logger.info(f"ICA_WES_WORKFLOW_VERSION_NAME={os.getenv('ICA_WES_WORKFLOW_VERSION_NAME')}")
        assert os.environ['ICA_BASE_URL'] == "http://localhost"
        assert os.environ['ICA_ACCESS_TOKEN'] == "mock"
        assert os.environ['ICA_WES_WORKFLOW_ID'] == TestConstant.wfl_id.value
        assert os.environ['ICA_WES_WORKFLOW_VERSION_NAME'] == TestConstant.version.value
        self.assertEqual(os.environ['ICA_BASE_URL'], "http://localhost")

        queue_urls = libsqs.sqs_client().list_queues()['QueueUrls']
        logger.info(f"SQS_QUEUE_URLS={queue_urls}")
        assert '4566' in queue_urls[0]
        logger.info(f"-" * 32)


class PipelineIntegrationTestCase(TestCase):
    pass
