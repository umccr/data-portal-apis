import json
from datetime import datetime

from django.utils.timezone import make_aware
from libiap.openapi import libwes
from mockito import when, verify

from data_portal.models import SequenceRun, Workflow
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.constant import WorkflowStatus, WorkflowHelper, WorkflowType
from data_processors.pipeline.lambdas import bcl_convert, demux_metadata
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase
from utils import libslack


class BCLConvertUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(BCLConvertUnitTests, self).setUp()
        when(demux_metadata).handler(...).thenReturn(
            {
                "settings_by_samples": [
                    {
                      "batch_name": "my-test-batch",
                      "samples": [
                          "PTC_EXPn200908LL_L2000001"
                      ],
                      "settings": {
                        "override_cycles": "Y100;I8N2;I8N2;Y100"
                      }
                    }
                ]
            }
        )

        wfl_helper = WorkflowHelper(WorkflowType.BCL_CONVERT.value)

        when(wfl_helper.get_ssm_key_input()).thenReturn(
            {
                "bcl_input_directory": {
                    "class": "Directory",
                    "location": "PLACEHOLDER"
                },
                "samplesheet": {
                    "class": "File",
                    "location": "PLACEHOLDER"
                },
                "settings_by_samples": [],
                "samplesheet_outdir": "samplesheets-by-assay-type",
                "ignore_missing_samples": True,
                "samplesheet_output_format": "v2",
                "bcl_sampleproject_subdirectories_bcl_conversion": True,
                "strict_mode_bcl_conversion": True,
                "delete_undetermined_indices_bcl_conversion": True,
                "runfolder_name": "PLACEHOLDER"
            }
        )

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_handler
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        workflow: dict = bcl_convert.handler({
            'gds_volume_name': mock_sqr.gds_volume_name,
            'gds_folder_path': mock_sqr.gds_folder_path,
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("-" * 32)
        logger.info("Example bcl_convert.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert bcl convert workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_handler_alt
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

        workflow = bcl_convert.handler({
            'gds_volume_name': mock_sqr.gds_volume_name,
            'gds_folder_path': mock_sqr.gds_folder_path,
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("-" * 32)
        logger.info("Example bcl_convert.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert bcl convert workflow launch success and save workflow runs in db
        success_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, success_bcl_convert_workflow_runs.count())

    def test_handler_metadata_validation_fail(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_handler_metadata_validation_fail
        """

        # This will fail metadata validation since there exists no samples
        when(demux_metadata).handler(...).thenReturn(
            {
                "sample": "",
                "override_cycles": "Y100;I8N2;I8N2;Y100",
                "type": "WGS",
                "assay": "TsqNano"
            }
        )

        result = bcl_convert.handler({
            'gds_volume_name': "gds_volume_name",
            'gds_folder_path': "gds_folder_path",
            'seq_run_id': "mock_sqr.run_id",
            'seq_name': "mock_sqr.name",
        }, None)

        logger.info("-" * 32)
        logger.info("Example bcl_convert.handler lambda output:")
        logger.info(json.dumps(result))

        # assert bcl convert workflow runs 0 in db
        no_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(0, no_bcl_convert_workflow_runs.count())

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_validate_metadata_blank_samples(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_blank_samples
        """
        mock_event = {
            'gds_volume_name': "bssh.xxxx",
            'gds_folder_path': "/Runs/cccc.gggg",
            'seq_run_id': "yyy",
            'seq_name': "zzz",
        }

        settings_by_samples = [
            {
                "batch_name": "my-batch",
                "samples": [],
                "settings": {
                    "override_cycles": "Y100;I8N2;I8N2;Y100"
                }
            }
        ]

        reason = bcl_convert.validate_metadata(mock_event, settings_by_samples)

        logger.info("-" * 32)
        logger.info(json.dumps(reason))

        self.assertIsNotNone(reason)

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_validate_no_batch_name(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_no_batch_name
        """
        mock_event = {
            'gds_volume_name': "bssh.xxxx",
            'gds_folder_path': "/Runs/cccc.gggg",
            'seq_run_id': "yyy",
            'seq_name': "zzz",
        }

        settings_by_samples = [
            {
                "samples": [
                    "PTC_EXPn200908LL_L2000002",
                    "PTC_EXPn200908LL_L2000003"
                ],
                "settings": {
                    "override_cycles": "Y100;I8N2;I8N2;Y100"
                }
            }
        ]

        reason = bcl_convert.validate_metadata(mock_event, settings_by_samples)

        logger.info("-" * 32)
        logger.info(json.dumps(reason))

        self.assertIsNotNone(reason)

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_validate_metadata_no_samples(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_no_samples
        """
        mock_event = {
            'gds_volume_name': "bssh.xxxx",
            'gds_folder_path': "/Runs/cccc.gggg",
            'seq_run_id': "yyy",
            'seq_name': "zzz",
        }

        settings_by_override_cycles = [
            {
                "batch_name": "my-no-samples-batch",
                "samples": [],
                "settings": {
                    "override_cycles": "Y100;I8N2;I8N2;Y100"
                }
            }
        ]

        reason = bcl_convert.validate_metadata(mock_event, settings_by_override_cycles)

        logger.info("-" * 32)
        logger.info(json.dumps(reason))

        self.assertIsNotNone(reason)

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_validate_metadata_no_override_cycles(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_no_override_cycles
        """
        mock_event = {
            'gds_volume_name': "bssh.xxxx",
            'gds_folder_path': "/Runs/cccc.gggg",
            'seq_run_id': "yyy",
            'seq_name': "zzz",
        }

        settings_by_override_cycles = [
            {
                "batch_name": "my-no-override-cycles-batch",
                "samples": [
                    "PTC_EXPn200908LL_L2000001",
                    "PTC_EXPn200908LL_L2000002",
                    "PTC_EXPn200908LL_L2000003"
                ],
                "settings": {}
            }
        ]

        reason = bcl_convert.validate_metadata(mock_event, settings_by_override_cycles)

        logger.info("-" * 32)
        logger.info(json.dumps(reason))

        self.assertIsNotNone(reason)

        # should call to slack webhook once
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_validate_metadata_pass(self):
        """
        python manage.py test data_processors.pipeline.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_pass
        """
        mock_event = {
            'gds_volume_name': "bssh.xxxx",
            'gds_folder_path': "/Runs/cccc.gggg",
            'seq_run_id': "yyy",
            'seq_name': "zzz",
        }

        settings_by_override_cycles = [
            {
                "batch_name": "my-passing-batch",
                "samples": [
                    "PTC_EXPn200908LL_L2000001",
                    "PTC_EXPn200908LL_L2000002",
                    "PTC_EXPn200908LL_L2000003"
                ],
                "settings": {
                    "override_cycles": "Y100;I8N2;I8N2;Y100"
                }
            }
        ]

        reason = bcl_convert.validate_metadata(mock_event, settings_by_override_cycles)

        logger.info("-" * 32)
        logger.info(json.dumps(reason))

        self.assertIsNone(reason)

        # should not call to slack webhook
        verify(libslack.http.client.HTTPSConnection, times=0).request(...)


class BCLConvertIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
