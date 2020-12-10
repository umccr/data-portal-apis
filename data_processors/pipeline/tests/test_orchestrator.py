import json
from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes, libgds
from mockito import when

from data_portal.models import Workflow, BatchRun, Batch
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class OrchestratorUnitTests(PipelineUnitTestCase):

    def test_workflow_output_not_json(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_workflow_output_not_json

        Should raise:
            [ERROR] JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
            ...
            ...
            json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes: line 3 column 13 (char 23)

        Storing models.Workflow.output into database should always be in JSON format.
        """
        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.name
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.output = """
        {
            'main/fastqs': {
                'location': "gds://{mock_workflow.wfr_id}/bclConversion_launch/try-1/out-dir-bclConvert",
                'basename': "out-dir-bclConvert",
                'nameroot': "",
                'nameext': "",
                'class': "Directory",
                'listing': []
            }
        }
        """
        try:
            orchestrator.next_step(mock_workflow, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_germline(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_germline
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            'main/fastq-directories': [
                {
                    'location': f"gds://{TestConstant.wfr_id.value}/outputs/OVERRIDE_CYCLES_ID_XZY",
                    'basename': "OVERRIDE_CYCLES_ID_XZY",
                    'nameroot': "",
                    'nameext': "",
                    'class': "Directory",
                    'listing': []
                },
            ]
        }
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        mock_file_list: libgds.FileListResponse = libgds.FileListResponse()
        volume = f"{TestConstant.wfr_id.value}"
        base = f"/outputs/OVERRIDE_CYCLES_ID_XZY/PROJECT"
        mock_files = [
            "NA12345 - 4KC_S7_R1_001.fastq.gz",
            "NA12345 - 4KC_S7_R2_001.fastq.gz",
            "PRJ111119_L1900000_S1_R1_001.fastq.gz",
            "PRJ111119_L1900000_S1_R2_001.fastq.gz",
            "MDX199999_L1999999_topup_S2_R1_001.fastq.gz",
            "MDX199999_L1999999_topup_S2_R2_001.fastq.gz",
            "L9111111_topup_S3_R1_001.fastq.gz",
            "L9111111_topup_S3_R2_001.fastq.gz",
            "NTC_L111111_S4_R1_001.fastq.gz",
            "NTC_L111111_S4_R2_001.fastq.gz",
        ]
        mock_file_list.items = []
        for mock_file in mock_files:
            mock_file_list.items.append(
                libgds.FileResponse(volume_name=volume, path=f"{base}/{mock_file}", name=mock_file),
            )
        when(libgds.FilesApi).list_files(...).thenReturn(mock_file_list)

        result = orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        self.assertIsNotNone(result)
        logger.info(f"Orchestrator lambda call output: \n{json.dumps(result)}")

        for b in Batch.objects.all():
            logger.info(f"BATCH: {b}")
        for br in BatchRun.objects.all():
            logger.info(f"BATCH_RUN: {br}")
        self.assertTrue(BatchRun.objects.all()[0].running)

    def test_germline_none(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_germline_none

        Similar to ^^ test case but BCL Convert output is None
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = None  # mock output is NONE while status is SUCCEEDED

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        try:
            orchestrator.handler({
                'wfr_id': TestConstant.wfr_id.value,
                'wfv_id': TestConstant.wfv_id.value,
            }, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_bcl_unknown_type(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_bcl_unknown_type

        Similar to ^^ test case but BCL Convert output is not list nor dict
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            'main/fastqs': "say, for example, cwl workflow output is some malformed string, oh well :("
        }

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        try:
            orchestrator.handler({
                'wfr_id': TestConstant.wfr_id.value,
                'wfv_id': TestConstant.wfv_id.value,
            }, None)
        except Exception as e:
            for b in Batch.objects.all():
                logger.info(f"BATCH: {b}")
            for br in BatchRun.objects.all():
                logger.info(f"BATCH_RUN: {br}")
            self.assertFalse(BatchRun.objects.all()[0].running)

            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(json.JSONDecodeError)


class OrchestratorIntegrationTests(PipelineIntegrationTestCase):
    pass
