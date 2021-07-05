import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.openapi import libwes
from mockito import when, verify

from data_portal.models import SequenceRun, Workflow, LabMetadata, LabMetadataType, LabMetadataAssay, \
    LabMetadataWorkflow
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.constant import WorkflowStatus
from data_processors.pipeline.lambdas import bcl_convert
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase
from utils import libslack


class BCLConvertUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(BCLConvertUnitTests, self).setUp()

        mock_labmetadata = LabMetadata()
        mock_labmetadata.library_id = "L2000001"
        mock_labmetadata.sample_id = "PTC_EXPn200908LL"
        mock_labmetadata.override_cycles = "Y100;I8N2;I8N2;Y100"
        mock_labmetadata.type = LabMetadataType.WGS.value
        mock_labmetadata.assay = LabMetadataAssay.TSQ_NANO.value
        mock_labmetadata.workflow = LabMetadataWorkflow.RESEARCH.value
        mock_labmetadata.save()

        when(bcl_convert).get_sample_names_from_samplesheet(...).thenReturn(
            [
                "PTC_EXPn200908LL_L2000001"
            ]
        )

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_handler
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
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_handler_alt
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
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_handler_metadata_validation_fail
        """

        # This will fail metadata validation since there exists no samples
        when(bcl_convert).get_sample_names_from_samplesheet(...).thenReturn(
            [
                ""
            ]
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

    def test_validate_metadata_blank_samples(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_blank_samples
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
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_no_batch_name
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
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_no_samples
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
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_no_override_cycles
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
                "settings": {
                    "adapter_read_1": "AAAACAACT"
                }
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
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_validate_metadata_pass
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
    # Comment @skip
    # export AWS_PROFILE=dev
    # run the test

    @skip
    def test_get_sample_names_from_samplesheet(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertIntegrationTests.test_get_sample_names_from_samplesheet
        """

        # SEQ-II validation dataset
        gds_volume = "umccr-raw-sequence-data-dev"
        samplesheet_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg/SampleSheet.csv"

        sample_names = bcl_convert.get_sample_names_from_samplesheet(
            gds_volume=gds_volume,
            samplesheet_path=samplesheet_path
        )

        self.assertIsNotNone(sample_names)
        self.assertTrue("PTC_SsCRE200323LL_L2000172_topup" in sample_names)

    @skip
    def test_get_metadata_df(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertIntegrationTests.test_get_metadata_df
        """

        # first need to populate LabMetadata tables
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'sheet': "2020"}, None)
        labmetadata.scheduled_update_handler({'sheet': "2021"}, None)

        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # SEQ-II validation dataset
        gds_volume = "umccr-raw-sequence-data-dev"
        samplesheet_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg/SampleSheet.csv"

        metadata_df = bcl_convert.get_metadata_df(
            gds_volume=gds_volume,
            samplesheet_path=samplesheet_path
        )

        logger.info("-" * 32)
        logger.info(f"\n{metadata_df}")

        self.assertTrue(not metadata_df.empty)
        self.assertTrue("PTC_SsCRE200323LL_L2000172_topup" in metadata_df["sample"].tolist())

        if "" in metadata_df["override_cycles"].unique().tolist():
            logger.info("-" * 32)
            logger.info("THERE SEEM TO BE BLANK OVERRIDE_CYCLES METADATA FOR SOME SAMPLES...")
            self.assertFalse("" in metadata_df["override_cycles"].tolist())
            # This probably mean need to fix data, look for corresponding Lab Metadata entry...
