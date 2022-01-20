from unittest import skip

from django.db.models import QuerySet

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.labmetadata import LabMetadata
from data_portal.tests.factories import TestConstant, LabMetadataFactory, LibraryRunFactory, TumorLabMetadataFactory, \
    TumorLibraryRunFactory, TumorNormalWorkflowFactory, WorkflowFactory
from data_processors.lims.lambdas import labmetadata
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger


class LibraryRunSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(LibraryRunSrvUnitTests, self).setUp()

    def test_create_or_update_library_run(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvUnitTests.test_create_or_update_library_run
        """
        mock_meta: LabMetadata = LabMetadataFactory()

        library_run = libraryrun_srv.create_or_update_library_run({
            'instrument_run_id': TestConstant.instrument_run_id.value,
            'run_id': TestConstant.run_id.value,
            'library_id': TestConstant.library_id_normal.value,
            'lane': 1,
            'override_cycles': TestConstant.override_cycles.value,
        })

        self.assertIsNotNone(library_run)
        self.assertEqual(LibraryRun.objects.count(), 1)
        self.assertEqual(library_run.override_cycles, mock_meta.override_cycles)
        self.assertIsNone(library_run.coverage_yield)
        self.assertTrue(library_run.valid_for_analysis)

    def test_update_library_run(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvUnitTests.test_update_library_run
        """
        mock_library_run: LibraryRun = LibraryRunFactory()

        library_run = libraryrun_srv.create_or_update_library_run({
            'instrument_run_id': TestConstant.instrument_run_id.value,
            'run_id': TestConstant.run_id.value,
            'library_id': TestConstant.library_id_normal.value,
            'lane': 1,
            'override_cycles': TestConstant.override_cycles.value,
            'qc_pass': True,
            'qc_status': "Pass",
            'coverage_yield': ">80%",
        })

        self.assertIsNotNone(library_run)
        self.assertEqual(LibraryRun.objects.count(), 1)
        self.assertEqual(library_run.override_cycles, mock_library_run.override_cycles)
        self.assertEqual(library_run.coverage_yield, ">80%")
        self.assertTrue(library_run.qc_pass)
        self.assertEqual(library_run.qc_status, "Pass")

    def test_link_library_runs_with_x_seq_workflow(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvUnitTests.test_link_library_runs_with_x_seq_workflow
        """
        mock_meta = TumorLabMetadataFactory()
        mock_library_run = TumorLibraryRunFactory()
        mock_workflow = TumorNormalWorkflowFactory()

        # So T/N is sequence-less workflow and to establish linking with LibraryRun

        mock_library_id = TestConstant.library_id_tumor.value

        library_run_list = libraryrun_srv.link_library_runs_with_x_seq_workflow([mock_library_id], mock_workflow)

        logger.info(library_run_list)

        self.assertTrue(len(library_run_list) > 0)
        self.assertIsNone(mock_workflow.sequence_run)

    def test_link_library_runs_with_x_seq_workflow_bcl_convert(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvUnitTests.test_link_library_runs_with_x_seq_workflow_bcl_convert
        """
        mock_meta = LabMetadataFactory()
        mock_library_run = LibraryRunFactory()
        mock_workflow = WorkflowFactory()

        # So BCL_Convert is sequence-aware workflow and to establish linking with LibraryRun

        mock_library_id = TestConstant.library_id_normal.value

        library_run_list = libraryrun_srv.link_library_runs_with_x_seq_workflow([mock_library_id], mock_workflow)

        logger.info(library_run_list)

        self.assertTrue(len(library_run_list) > 0)
        self.assertIsNotNone(mock_workflow.sequence_run)


class LibraryRunSrvIntegrationTests(PipelineIntegrationTestCase):
    # Comment @skip
    # export AWS_PROFILE=dev
    # run the test

    @skip
    def test_create_library_run_from_sequence(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvIntegrationTests.test_create_library_run_from_sequence
        """

        # populate test db with LabMetadata
        stat = labmetadata.scheduled_update_handler({'event': "LibraryRunSrvIntegrationTests", 'truncate': False}, None)
        logger.info(f"{stat}")

        # SEQ-II validation dataset
        gds_volume_name = "umccr-raw-sequence-data-dev"
        gds_folder_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg"
        sample_sheet_name = "SampleSheet.csv"

        library_run_list = libraryrun_srv.create_library_run_from_sequence({
            'instrument_run_id': "200612_A01052_0017_BH5LYWDSXY",
            'run_id': "r.Uvlx2DEIME-KH0BRyF9XBg",
            'gds_folder_path': gds_folder_path,
            'gds_volume_name': gds_volume_name,
            'sample_sheet_name': sample_sheet_name,
        })

        self.assertEqual(37, len(library_run_list))
        self.assertEqual(37, LibraryRun.objects.count())

        qs: QuerySet = LibraryRun.objects.filter(library_id="L2000172_topup")
        self.assertFalse(qs.exists())  # assert _topup is stripped

        qs = LibraryRun.objects.filter(library_id="L2000172")
        lib_run: LibraryRun = qs.get()

        logger.info("-" * 32)
        logger.info(lib_run)
        logger.info(lib_run.override_cycles)
        self.assertEqual(1, lib_run.lane)
        self.assertEqual("Y151;I8N2;U10;Y151", lib_run.override_cycles)

    @skip
    def test_create_library_run_from_sequence_no_lane_number(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvIntegrationTests.test_create_library_run_from_sequence_no_lane_number
        """

        # populate test db with LabMetadata
        stat = labmetadata.scheduled_update_handler({'event': "LibraryRunSrvIntegrationTests", 'truncate': False}, None)
        logger.info(f"{stat}")

        gds_volume_name = "umccr-raw-sequence-data-dev"
        gds_folder_path = "/200110_A00130_0128_AHMF7VDSXX"
        sample_sheet_name = "SampleSheet-test.csv"

        library_run_list = libraryrun_srv.create_library_run_from_sequence({
            'instrument_run_id': "200110_A00130_0128_AHMF7VDSXX",
            'run_id': "",
            'gds_folder_path': gds_folder_path,
            'gds_volume_name': gds_volume_name,
            'sample_sheet_name': sample_sheet_name,
        })

        self.assertEqual(52, len(library_run_list))
        self.assertEqual(52, LibraryRun.objects.count())

        qs = LibraryRun.objects.filter(library_id="L2000001")
        # we expect 4 entries (one per lane as defined by RunInfo.xml)
        self.assertEqual(4, qs.count())

    @skip
    def test_create_library_run_from_sequence_no_samplesheet(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_libraryrun_srv.LibraryRunSrvIntegrationTests.test_create_library_run_from_sequence_no_samplesheet
        """

        gds_volume_name = "umccr-raw-sequence-data-dev"
        gds_folder_path = "/200110_A00130_0128_AHMF7VDSXX"
        sample_sheet_name = "SampleSheet-not-exist.csv"

        library_run_list = libraryrun_srv.create_library_run_from_sequence({
            'instrument_run_id': "200110_A00130_0128_AHMF7VDSXX",
            'run_id': "",
            'gds_folder_path': gds_folder_path,
            'gds_volume_name': gds_volume_name,
            'sample_sheet_name': sample_sheet_name,
        })

        self.assertEqual(0, len(library_run_list))
        self.assertEqual(0, LibraryRun.objects.count())
        self.assertRaises(ValueError)
