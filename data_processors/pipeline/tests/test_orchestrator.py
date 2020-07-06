import json
from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes, libgds
from mockito import when

from data_portal.models import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class OrchestratorUnitTests(PipelineUnitTestCase):

    def test_workflow_output_not_json(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_workflow_output_not_json
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
        """
        Should raise similar to:

        [ERROR] JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
        Traceback (most recent call last):
          File "/var/task/data_processors/pipeline/tests/test_orchestrator.py", line 59, in test_workflow_output_not_json
            orchestrator.next_step(mock_workflow, None)
          File "/var/task/data_processors/pipeline/lambdas/orchestrator.py", line 86, in next_step
            output_gds_path: str = libjson.loads(this_workflow.output)['main/fastqs']['location']
          File "/var/task/utils/libjson.py", line 11, in loads
            return json.loads(data)
          File "/var/lang/lib/python3.8/json/__init__.py", line 357, in loads
            return _default_decoder.decode(s)
          File "/var/lang/lib/python3.8/json/decoder.py", line 337, in decode
            obj, end = self.raw_decode(s, idx=_w(s, 0).end())
          File "/var/lang/lib/python3.8/json/decoder.py", line 353, in raw_decode
            obj, end = self.scan_once(s, idx)
        json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes: line 3 column 13 (char 23)

        Storing models.Workflow.output into database should always be in JSON format.
        """
        try:
            orchestrator.next_step(mock_workflow, None)
        except Exception as e:
            logger.exception(f"THIS 'json.decoder.JSONDecodeError' EXCEPTION IS INTENTIONAL FOR TEST. NOT ERROR. \n{e}")
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
            'main/fastqs': {
                'location': f"gds://{TestConstant.wfr_id.value}/bclConversion_launch/try-1/out-dir-bclConvert",
                'basename': "out-dir-bclConvert",
                'nameroot': "",
                'nameext': "",
                'class': "Directory",
                'listing': []
            }
        }
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        mock_file_list: libgds.FileListResponse = libgds.FileListResponse()
        mock_file_list.items = [
            libgds.FileResponse(name="NA12345 - 4KC_S7_R1_001.fastq.gz"),
            libgds.FileResponse(name="NA12345 - 4KC_S7_R2_001.fastq.gz"),
            libgds.FileResponse(name="PRJ111119_L1900000_S1_R1_001.fastq.gz"),
            libgds.FileResponse(name="PRJ111119_L1900000_S1_R2_001.fastq.gz"),
            libgds.FileResponse(name="MDX199999_L1999999_topup_S2_R1_001.fastq.gz"),
            libgds.FileResponse(name="MDX199999_L1999999_topup_S2_R2_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R1_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R2_001.fastq.gz"),
        ]
        when(libgds.FilesApi).list_files(...).thenReturn(mock_file_list)

        orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        workflows = Workflow.objects.all()
        self.assertEqual(5, workflows.count())


class OrchestratorIntegrationTests(PipelineIntegrationTestCase):
    pass
