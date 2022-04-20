import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.openapi import libwes
from libumccr import libslack
from mockito import when, verify

from data_portal.models.labmetadata import LabMetadata, LabMetadataType, LabMetadataAssay, LabMetadataWorkflow
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import SequenceRunFactory, TestConstant, LibraryRunFactory, WorkflowFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.lambdas import bcl_convert
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase
from data_processors.pipeline.tools import liborca


class BCLConvertUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(BCLConvertUnitTests, self).setUp()

        mock_labmetadata = LabMetadata()
        mock_labmetadata.library_id = "L2000001_topup"
        mock_labmetadata.sample_id = "PTC_EXPn200908LL"
        mock_labmetadata.override_cycles = "Y100;I8N2;I8N2;Y100"
        mock_labmetadata.type = LabMetadataType.WGS.value
        mock_labmetadata.assay = LabMetadataAssay.TSQ_NANO.value
        mock_labmetadata.workflow = LabMetadataWorkflow.RESEARCH.value
        mock_labmetadata.save()

        when(liborca).get_sample_names_from_samplesheet(...).thenReturn(
            [
                "PTC_EXPn200908LL_L2000001_topup"
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

    def test_libraryrun_workflow_link(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_libraryrun_workflow_link
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_libraryrun: LibraryRun = LibraryRunFactory()

        # Change library_id to match metadata
        mock_libraryrun.library_id = "L2000001"
        mock_libraryrun.save()
        result: dict = bcl_convert.handler({
            'gds_volume_name': mock_sqr.gds_volume_name,
            'gds_folder_path': mock_sqr.gds_folder_path,
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("-" * 32)
        logger.info("Example bcl_convert.handler lambda output:")
        logger.info(json.dumps(result))

        # assert bcl convert workflow launch success and save workflow run in db
        workflow = Workflow.objects.get(id=result['id'])

        # Grab library run for particular workflow
        library_run_in_workflows = workflow.libraryrun_set.all()

        self.assertEqual(1, library_run_in_workflows.count())

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
        when(liborca).get_sample_names_from_samplesheet(...).thenReturn(
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

    def test_get_settings_by_instrument_type_assay(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="mock",
            sample_type="mock",
            assay="mock",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertIsNotNone(settings)
        self.assertIsInstance(settings, dict)
        self.assertEqual(len(settings), 1)
        self.assertIn("minimum_adapter_overlap", settings.keys())
        self.assertEqual(settings['minimum_adapter_overlap'], 3)

    def test_get_settings_by_instrument_type_assay_10X(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_10X
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="mock",
            sample_type="10X",
            assay="10X-5prime-expression",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertEqual(len(settings), 6)
        self.assertEqual(settings['minimum_adapter_overlap'], 3)
        self.assertEqual(settings['minimum_trimmed_read_length'], 8)
        self.assertEqual(settings['mask_short_reads'], 8)

    def test_get_settings_by_instrument_type_assay_TSO(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_TSO
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="NovaSeq",
            sample_type="ctDNA",
            assay="ctTSO",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertEqual(len(settings), 6)
        self.assertEqual(settings['minimum_adapter_overlap'], 3)
        self.assertEqual(settings['minimum_trimmed_read_length'], 35)
        self.assertEqual(settings['mask_short_reads'], 35)
        self.assertEqual(settings['adapter_behavior'], "trim")

    def test_get_settings_by_instrument_type_assay_TsqNano(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_TsqNano
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="mock",
            sample_type="mock",
            assay="TsqNano",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertEqual(len(settings), 3)

    def test_get_settings_by_instrument_type_assay_NebDNA(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_NebDNA
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="mock",
            sample_type="mock",
            assay="NebDNA",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertEqual(len(settings), 3)

    def test_get_settings_by_instrument_type_assay_PCR(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_PCR
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="mock",
            sample_type="mock",
            assay="PCR-Free-Tagmentation",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertEqual(len(settings), 3)

    def test_get_settings_by_instrument_type_assay_agilent(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_agilent
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="MiSeq",
            sample_type="mock",
            assay="AgSsCRE",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertEqual(len(settings), 3)

    def test_get_settings_by_instrument_type_assay_external(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertUnitTests.test_get_settings_by_instrument_type_assay_external
        """
        settings = bcl_convert.get_settings_by_instrument_type_assay(
            instrument="NovaSeq",
            sample_type="Metagenm",
            assay="IlmnDNAprep",
        )

        logger.info("-" * 32)
        logger.info(settings)

        self.assertIsNotNone(settings)
        self.assertIsInstance(settings, dict)
        self.assertEqual(len(settings), 1)
        self.assertIn("minimum_adapter_overlap", settings.keys())
        self.assertEqual(settings['minimum_adapter_overlap'], 3)


class BCLConvertIntegrationTests(PipelineIntegrationTestCase):
    # Comment @skip
    # export AWS_PROFILE=dev
    # run the test

    @skip
    def test_get_metadata_df(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_bcl_convert.BCLConvertIntegrationTests.test_get_metadata_df
        """

        # first need to populate LabMetadata tables
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'event': "test_get_metadata_df"}, None)

        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # SEQ-II validation dataset
        mock_bcl_workflow: Workflow = WorkflowFactory()
        mock_sqr: SequenceRun = mock_bcl_workflow.sequence_run
        mock_sqr.run_id = "r.Uvlx2DEIME-KH0BRyF9XBg"
        mock_sqr.instrument_run_id = "200612_A01052_0017_BH5LYWDSXY"
        mock_sqr.gds_volume_name = "umccr-raw-sequence-data-dev"
        mock_sqr.gds_folder_path = f"/{mock_sqr.instrument_run_id}_{mock_sqr.run_id}"
        mock_sqr.sample_sheet_name = "SampleSheet.csv"
        mock_sqr.name = mock_sqr.instrument_run_id
        mock_sqr.save()

        mock_library_run = LibraryRun(
            instrument_run_id=mock_sqr.instrument_run_id,
            run_id=mock_sqr.run_id,
            library_id="L2000199",
            lane=1,
            override_cycles="Y151;I8N2;U10;Y151",
        )
        mock_library_run.save()

        samplesheet_path = f"{mock_sqr.gds_folder_path}/{mock_sqr.sample_sheet_name}"

        metadata_df = bcl_convert.get_metadata_df(
            gds_volume=mock_sqr.gds_volume_name,
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

        library_id_list = metadata_df["library_id"].tolist()
        library_run_list = libraryrun_srv.link_library_runs_with_x_seq_workflow(library_id_list, mock_bcl_workflow)
        self.assertIsNotNone(library_run_list)
        self.assertEqual(1, len(library_run_list))
        self.assertEqual(mock_library_run.library_id, library_run_list[0].library_id)

        library_run_in_workflows = mock_bcl_workflow.libraryrun_set.all()
        self.assertEqual(1, library_run_in_workflows.count())
