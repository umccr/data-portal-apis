import json
from datetime import datetime

from libica.openapi import libwes
from mockito import when

from data_portal.tests.factories import TestConstant
from data_processors.pipeline.constant import WorkflowStatus, WorkflowRunEventType
from data_processors.pipeline.lambdas import wes_handler
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class WESHandlerUnitTests(PipelineUnitTestCase):

    def test_openapi_type(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_openapi_type

        Monitor mock container if you like:  docker logs -f iap_mock_wes_1
            [HTTP SERVER] get /v1/workflows/runs/wfr.anything_work Request received
        """
        config = libwes.Configuration(
            host="http://localhost",
            api_key={
                'Authorization': "mock"
            },
            api_key_prefix={
                'Authorization': "Bearer"
            },
        )
        with libwes.ApiClient(config) as api_client:
            run_api = libwes.WorkflowRunsApi(api_client)
            wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id="wfr.anything_work")
            logger.info((wfl_run.output, type(wfl_run.output)))
            logger.info((wfl_run.time_stopped, type(wfl_run.time_stopped)))
            logger.info((wfl_run.status, type(wfl_run.status)))

        self.assertTrue(isinstance(wfl_run.output, dict))
        self.assertTrue(isinstance(wfl_run.time_stopped, datetime))
        self.assertTrue(isinstance(wfl_run.status, str))

    def test_launch(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_launch

        Monitor mock container if you like:  docker logs -f iap_mock_wes_1
            [HTTP SERVER] post /v1/workflows/wfl.any_work_hitting_prism_dynamic_mock/versions/v1:launch Request received
        """
        wfl_run: dict = wes_handler.launch({
            'workflow_id': "wfl.any_work_hitting_prism_dynamic_mock",
            'workflow_version': "v1",
            'workflow_run_name': "umccr__test__run",
            'workflow_input': {}
        }, None)
        self.assertIsNotNone(wfl_run)
        self.assertTrue(isinstance(wfl_run, dict))

    def test_launch_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_launch_alt
        """
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.name = "umccr__test__launch__alt"
        when(libwes.WorkflowVersionsApi).launch_workflow_version(...).thenReturn(mock_wfl_run)

        wfl_run: dict = wes_handler.launch({
            'workflow_id': f"{TestConstant.wfl_id.value}",
            'workflow_version': f"{TestConstant.version.value}",
            'workflow_run_name': mock_wfl_run.name,
            'workflow_input': {}
        }, None)

        self.assertEqual(wfl_run['id'], TestConstant.wfr_id.value)

    def test_launch_lambda_return_serialized(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_launch_lambda_return_serialized
        """
        wfl_run: dict = wes_handler.launch({
            'workflow_id': "wfl.any_work_hitting_prism_dynamic_mock",
            'workflow_version': "v1",
            'workflow_run_name': "umccr__test__run",
            'workflow_input': {}
        }, None)
        self.assertIsNotNone(wfl_run)
        self.assertTrue(isinstance(wfl_run, dict))

        logger.info("-"*32)
        logger.info("Workflow run response dict:")
        logger.info(wfl_run)

        logger.info("Workflow run response dict should be lambda serializable:")
        # https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
        # If the handler returns objects that can't be serialized by json.dumps, the runtime returns an error.
        lambda_serialized_json = json.dumps(wfl_run)  # test, must be able to json.dumps on the return dict
        logger.info(lambda_serialized_json)

        self.assertTrue(isinstance(wfl_run['time_started'], str))

    def test_get_workflow_run(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_get_workflow_run
        """
        wfl_run_status = wes_handler.get_workflow_run({
            'wfr_id': "wfr.xxx",
            'wfr_event': {
                'event_type': "RunSucceeded",
                'event_details': {},
                'timestamp': "2020-06-24T11:27:35.1268588Z"
            }
        }, None)
        self.assertIsNotNone(wfl_run_status)
        self.assertTrue(isinstance(wfl_run_status, dict))

    def test_get_workflow_run_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_get_workflow_run_alt
        """
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.name = "umccr__test_get_workflow_run_alt"
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = datetime.utcnow()
        mock_wfl_run.output = {'main/fastq': "gds://volume/some/output"}
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        wfl_run_status = wes_handler.get_workflow_run({
            'wfr_id': f"{mock_wfl_run.id}",
        }, None)

        self.assertEqual(wfl_run_status['status'], WorkflowStatus.SUCCEEDED.value)

    def test_get_workflow_run_alt2(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_get_workflow_run_alt2
        """
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.name = "umccr__test_get_workflow_run_alt2"
        mock_wfl_run.status = WorkflowStatus.RUNNING.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        wfl_run_status = wes_handler.get_workflow_run({
            'wfr_id': f"{mock_wfl_run.id}",
            'wfr_event': {
                'event_type': WorkflowRunEventType.RUNSTARTED.value,
                'event_details': {},
                'timestamp': "2020-06-24T11:27:35.1268588Z"
            }
        }, None)

        self.assertEqual(wfl_run_status['status'], WorkflowStatus.RUNNING.value)

    def test_get_workflow_run_alt3(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_get_workflow_run_alt3
        """
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.name = "umccr__test_get_workflow_run_alt3"
        mock_wfl_run.status = WorkflowStatus.ABORTED.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        wfl_run_status = wes_handler.get_workflow_run({
            'wfr_id': f"{mock_wfl_run.id}",
            'wfr_event': {
                'event_type': WorkflowRunEventType.RUNABORTED.value,
                'event_details': {},
                'timestamp': "2020-06-24T11:27:35.1268588Z"
            }
        }, None)

        self.assertEqual(wfl_run_status['status'], WorkflowStatus.ABORTED.value)

    def test_get_workflow_run_alt4(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_get_workflow_run_alt4
        """
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.name = "umccr__test_get_workflow_run_alt4"
        mock_wfl_run.status = WorkflowStatus.RUNNING.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        mock_hist = libwes.WorkflowRunHistoryEventList()
        mock_hist_event1 = libwes.WorkflowRunHistoryEvent()
        mock_hist_event1.event_id = 0
        mock_hist_event1.event_type = "RunStarted"
        mock_hist_event1.event_details = {}
        mock_hist_event1.timestamp = datetime.utcnow()
        mock_hist_event2: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_hist_event2.event_id = 46586
        mock_hist_event2.timestamp = datetime.utcnow()
        mock_hist_event2.event_type = WorkflowRunEventType.RUNFAILED.value
        mock_hist_event2.event_details = {
            'error': "Workflow.Failed",
            'cause': "Run Failed. Reason: task: [samplesheetSplit_launch] details: [Failed to submit TES Task. "
                     "Reason [(500)\nReason: Internal Server Error\nHTTP response headers: "
                     "HTTPHeaderDict({'Date': 'Mon, 29 Jun 2020 07:37:16 GMT', 'Content-Type': 'application/json', "
                     "'Server': 'Kestrel', 'Transfer-Encoding': 'chunked'})\nHTTP response body: "
                     "{\"code\":\"\",\"message\":\"We had an unexpected issue.  Please try your request again.  "
                     "The issue has been logged and we are looking into it.\"}\n]]"
        }
        mock_hist.items = [mock_hist_event1, mock_hist_event2]
        when(libwes.WorkflowRunsApi).list_workflow_run_history(...).thenReturn(mock_hist)

        wfl_run_status = wes_handler.get_workflow_run({
            'wfr_id': f"{mock_wfl_run.id}",
            'wfr_event': {
                'event_type': WorkflowRunEventType.RUNFAILED.value,
                'event_details': {},
                'timestamp': "2020-06-24T11:27:35.1268588Z"
            }
        }, None)

        self.assertEqual(wfl_run_status['status'], WorkflowStatus.FAILED.value)

    def test_get_workflow_run_alt5(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_wes_handler.WESHandlerUnitTests.test_get_workflow_run_alt5
        """
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.name = "umccr__test_get_workflow_run_alt5"
        mock_wfl_run.status = WorkflowStatus.RUNNING.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        mock_hist = libwes.WorkflowRunHistoryEventList()
        mock_hist_event1 = libwes.WorkflowRunHistoryEvent()
        mock_hist_event1.event_id = 0
        mock_hist_event1.event_type = "RunStarted"
        mock_hist_event1.event_details = {}
        mock_hist_event1.timestamp = datetime.utcnow()

        # what if run history last event stuck at "TaskFailed" but ENS fires wes.run event with "RunFailed"... scream!!
        # the show must go on...
        mock_hist_event2: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_hist_event2.event_id = 46586
        mock_hist_event2.timestamp = datetime.utcnow()
        mock_hist_event2.event_type = "TaskFailed"
        mock_hist_event2.event_details = {
            "duration": 33.614538,
            "tryNumber": 4,
            "taskStatus": "Failed",
            "errorCause": "[job bclConvert-arguments.cwl] job error: TES task trn.xxx, status: Failed",
            "absolutePath": "/bclConversion_collect",
            "stateName": "bclConversion_collect"
        }
        mock_hist.items = [mock_hist_event1, mock_hist_event2]
        when(libwes.WorkflowRunsApi).list_workflow_run_history(...).thenReturn(mock_hist)

        wfl_run_status = wes_handler.get_workflow_run({
            'wfr_id': f"{mock_wfl_run.id}",
            'wfr_event': {
                'event_type': WorkflowRunEventType.RUNFAILED.value,
                'event_details': {
                    'Error': "Workflow.Failed",
                    'Cause': "Run Failed. Something incomprehensible failure had happened :( tsk tsk..."
                },
                'timestamp': "2020-06-24T11:27:35.1268588Z"  # <<< take event timestamp as time_stopped or end time
            }
        }, None)

        self.assertEqual(wfl_run_status['status'], WorkflowStatus.FAILED.value)
        self.assertEqual(wfl_run_status['end'], "2020-06-24T11:27:35.1268588Z")
