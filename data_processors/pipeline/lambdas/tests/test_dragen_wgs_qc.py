import json
from datetime import datetime

from django.utils.timezone import make_aware
from libica.openapi import libwes
from mockito import when

from data_portal.models import Workflow, SequenceRun, BatchRun
from data_portal.tests.factories import SequenceRunFactory, TestConstant, BatchRunFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType, WorkflowHelper
from data_processors.pipeline.lambdas import dragen_wgs_qc
from data_processors.pipeline.services import metadata_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase
from utils import libjson, libssm


class DragenWgsQcUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler
        """
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ0001")

        workflow: dict = dragen_wgs_qc.handler({
            "library_id": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": 1,
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
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WGS_QC workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler_alt
        """
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ0001")

        mock_wfr: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_wfr.id = TestConstant.wfr_id.value
        mock_wfr.time_started = make_aware(datetime.utcnow())
        mock_wfr.status = WorkflowStatus.RUNNING.value
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfr.workflow_version = workflow_version
        when(libwes.WorkflowVersionsApi).launch_workflow_version(...).thenReturn(mock_wfr)

        workflow: dict = dragen_wgs_qc.handler({
            "library_id": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": 1,
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
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WGS_QC workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_handler_skipped(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler_skipped
        """
        mock_sqr: SequenceRun = SequenceRunFactory()
        mock_batch_run: BatchRun = BatchRunFactory()

        wfl_helper = WorkflowHelper(WorkflowType.DRAGEN_WGS_QC)

        mock_dragen_wgs_qc = Workflow()
        mock_dragen_wgs_qc.type_name = WorkflowType.DRAGEN_WGS_QC.name
        mock_dragen_wgs_qc.wfl_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
        mock_dragen_wgs_qc.version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())
        mock_dragen_wgs_qc.sample_name = "SAMPLE_NAME"
        mock_dragen_wgs_qc.sequence_run = mock_sqr
        mock_dragen_wgs_qc.batch_run = mock_batch_run
        mock_dragen_wgs_qc.start = make_aware(datetime.utcnow())
        mock_dragen_wgs_qc.input = libjson.dumps({'mock': "MOCK_INPUT_JSON"})
        mock_dragen_wgs_qc.save()

        result: dict = dragen_wgs_qc.handler({
            "library_id": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": 1,
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
            "batch_run_id": mock_batch_run.id,
        }, None)

        logger.info("-" * 32)
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(result))
        self.assertEqual('SKIPPED', result['status'])

        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_sqs_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_sqs_handler
        """

        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ0001")

        mock_job = {
            "library_id": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": 1,
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

        results = dragen_wgs_qc.sqs_handler(mock_event, None)
        logger.info("-" * 32)
        logger.info("Example dragen_wgs_qc.sqs_handler lambda output:")
        logger.info(json.dumps(results))

        self.assertEqual(len(results), 1)


class DragenWgsQcIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
