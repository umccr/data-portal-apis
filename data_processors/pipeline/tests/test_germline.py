import json
from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes
from mockito import when

from data_portal.models import Workflow, SequenceRun, BatchRun
from data_portal.tests.factories import SequenceRunFactory, TestConstant, BatchRunFactory
from data_processors.pipeline.constant import WorkflowStatus, WorkflowType, WorkflowHelper
from data_processors.pipeline.lambdas import germline
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase
from utils import libjson, libssm


class GermlineUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_germline.GermlineUnitTests.test_handler
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        workflow: dict = germline.handler({
            "sample_name": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": int,
                    "read_1": {
                      "class": "File",
                      "location": "gds://path/to/read_1.fastq.gz"
                    },
                    "read_2": {
                      "class": "File",
                      "location": "gds://path/to/read_2.fastq.gz"
                    }
                }
            ],
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
        }, None)

        logger.info("-" * 32)
        logger.info("Example germline.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert germline workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.tests.test_germline.GermlineUnitTests.test_handler_alt
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_wfr: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_wfr.id = TestConstant.wfr_id.value
        mock_wfr.time_started = make_aware(datetime.utcnow())
        mock_wfr.status = WorkflowStatus.RUNNING.value
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfr.workflow_version = workflow_version
        when(libwes.WorkflowVersionsApi).launch_workflow_version(...).thenReturn(mock_wfr)

        workflow: dict = germline.handler({
            "sample_name": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": int,
                    "read_1": {
                        "class": "File",
                        "location": "gds://path/to/read_1.fastq.gz"
                    },
                    "read_2": {
                        "class": "File",
                        "location": "gds://path/to/read_2.fastq.gz"
                    }
                }
            ],
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
        }, None)
        logger.info("-" * 32)
        logger.info("Example germline.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert germline workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_handler_skipped(self):
        """
        python manage.py test data_processors.pipeline.tests.test_germline.GermlineUnitTests.test_handler_skipped
        """
        mock_sqr: SequenceRun = SequenceRunFactory()
        mock_batch_run: BatchRun = BatchRunFactory()

        wfl_helper = WorkflowHelper(WorkflowType.GERMLINE.value)

        mock_germline = Workflow()
        mock_germline.type_name = WorkflowType.GERMLINE.name
        mock_germline.wfl_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
        mock_germline.version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())
        mock_germline.sample_name = "SAMPLE_NAME"
        mock_germline.sequence_run = mock_sqr
        mock_germline.batch_run = mock_batch_run
        mock_germline.start = make_aware(datetime.utcnow())
        mock_germline.input = libjson.dumps({'mock': "MOCK_INPUT_JSON"})
        mock_germline.save()

        result: dict = germline.handler({
            "sample_name": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": int,
                    "read_1": {
                        "class": "File",
                        "location": "gds://path/to/read_1.fastq.gz"
                    },
                    "read_2": {
                        "class": "File",
                        "location": "gds://path/to/read_2.fastq.gz"
                    }
                }
            ],
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
        }, None)

        logger.info("-" * 32)
        logger.info("Example germline.handler lambda output:")
        logger.info(json.dumps(result))
        self.assertEqual('SKIPPED', result['status'])

        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_sqs_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_germline.GermlineUnitTests.test_sqs_handler
        """

        mock_job = {
                      "sample_name": "SAMPLE_NAME",
                      "fastq_list_rows": [
                          {
                              "rgid": "index1.index2.lane",
                              "rgsm": "sample_name",
                              "rglb": "sample_library",
                              "lane": int,
                              "read_1": {
                                  "class": "File",
                                  "location": "gds://path/to/read_1.fastq.gz"
                              },
                              "read_2": {
                                  "class": "File",
                                  "location": "gds://path/to/read_2.fastq.gz"
                              }
                          }
                      ],
                      "seq_run_id": "sequence run id",
                      "seq_name": "sequence run name",
                      "batch_run_id": 1
                      }

        mock_event = {
            'Records': [
                {
                    'messageId': "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                    'body': libjson.dumps(mock_job),
                    'messageAttributes': {},
                    'md5OfBody': "e4e68fb7bd0e697a0ae8f1bb342846b3",
                    'eventSource': "aws:sqs",
                    'eventSourceARN': "arn:aws:sqs:us-east-2:123456789012:fifo.fifo",
                },
            ]
        }

        results = germline.sqs_handler(mock_event, None)
        logger.info("-" * 32)
        logger.info("Example germline.sqs_handler lambda output:")
        logger.info(json.dumps(results))

        self.assertEqual(len(results), 1)


class GermlineIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
