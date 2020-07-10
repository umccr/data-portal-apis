import json
from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes
from mockito import when

from data_portal.models import Workflow, SequenceRun
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.constant import WorkflowStatus
from data_processors.pipeline.lambdas import germline
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class GermlineUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_germline.GermlineUnitTests.test_handler
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        workflow: dict = germline.handler({
            'fastq1': "SAMPLE_NAME_S1_R1_001.fastq.gz",
            'fastq2': "SAMPLE_NAME_S1_R2_001.fastq.gz",
            'sample_name': "SAMPLE_NAME",
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("-"*32)
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
            'fastq1': "SAMPLE_NAME_S1_R1_001.fastq.gz",
            'fastq2': "SAMPLE_NAME_S1_R2_001.fastq.gz",
            'sample_name': "SAMPLE_NAME",
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("-"*32)
        logger.info("Example germline.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert germline workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())


class GermlineIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
