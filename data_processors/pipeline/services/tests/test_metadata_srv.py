from typing import List

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.tests import factories
from data_portal.tests.factories import TestConstant, LibraryRunFactory, DragenWgsQcWorkflowFactory
from data_processors.pipeline.services import metadata_srv, libraryrun_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class MetadataSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(MetadataSrvUnitTests, self).setUp()

    def test_get_metadata_by_library_id(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_metadata_by_library_id
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        meta = metadata_srv.get_metadata_by_library_id(TestConstant.library_id_normal.value)
        self.assertEqual(meta.library_id, mock_meta.library_id)

    def test_get_metadata_by_library_id_not_found(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_metadata_by_library_id_not_found
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        meta = metadata_srv.get_metadata_by_library_id("L_NOT_EXIST")
        self.assertIsNone(meta)

    def test_filter_metadata_by_library_id(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_filter_metadata_by_library_id
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        meta_list = metadata_srv.filter_metadata_by_library_id(TestConstant.library_id_normal.value)
        self.assertEqual(len(meta_list), 1)

    def test_get_metadata_by_sample_library_name_as_in_samplesheet(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_metadata_by_sample_library_name_as_in_samplesheet
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        meta: LabMetadata = metadata_srv.get_metadata_by_sample_library_name_as_in_samplesheet(
            f"{TestConstant.sample_id.value}_{TestConstant.library_id_normal.value}"
        )
        self.assertEqual(meta.library_id, mock_meta.library_id)

    def test_get_tn_metadata_by_qc_runs(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_tn_metadata_by_qc_runs
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        mock_library_run: LibraryRun = LibraryRunFactory()
        mock_workflow = DragenWgsQcWorkflowFactory()
        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_library_run.library_id],
            workflow=mock_workflow
        )

        meta_list, _ = metadata_srv.get_tn_metadata_by_qc_runs([mock_workflow])
        logger.info(meta_list)
        self.assertEqual(meta_list[0].subject_id, mock_meta.subject_id)

    def test_get_wts_metadata_by_subject(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_wts_metadata_by_subject
        """
        mock_wts_meta: LabMetadata = factories.WtsTumorLabMetadataFactory()
        meta_list: List[LabMetadata] = metadata_srv.get_wts_metadata_by_subject(TestConstant.subject_id.value)
        for meta in meta_list:
            logger.info(meta.library_id)
            self.assertEqual(meta.library_id, TestConstant.wts_library_id_tumor.value)

    def test_get_sorted_library_id_by_sequencing_time(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_sorted_library_id_by_sequencing_time
        """
        mock_sqr: SequenceRun = factories.SequenceRunFactory()
        mock_wts_meta: LabMetadata = factories.WtsTumorLabMetadataFactory()
        mock_wts_lbr: LibraryRun = factories.WtsTumorLibraryRunFactory()

        recent_lib_id = metadata_srv.get_sorted_library_id_by_sequencing_time([TestConstant.wts_library_id_tumor.value])
        logger.info(recent_lib_id)
        self.assertEqual(recent_lib_id, TestConstant.wts_library_id_tumor.value)

    def test_get_sorted_library_id_by_sequencing_time_recurring_subject(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_sorted_library_id_by_sequencing_time_recurring_subject
        """
        # first time sequencing
        mock_sqr: SequenceRun = factories.SequenceRunFactory()
        mock_wts_meta: LabMetadata = factories.WtsTumorLabMetadataFactory()
        mock_wts_lbr: LibraryRun = factories.WtsTumorLibraryRunFactory()

        # second time sequencing (recurring subject)
        mock_wts_meta_2: LabMetadata = factories.WtsTumorLabMetadataFactory2()
        mock_wts_lbr_2: LibraryRun = factories.WtsTumorLibraryRunFactory2()
        mock_sqr_2: SequenceRun = factories.SequenceRunFactory2()

        # assert our test database has 2 mock instances
        self.assertEqual(SequenceRun.objects.count(), 2)
        self.assertEqual(LibraryRun.objects.count(), 2)
        self.assertEqual(LabMetadata.objects.count(), 2)

        eval_lib_ids = [TestConstant.wts_library_id_tumor.value, mock_wts_meta_2.library_id]

        recent_lib_id = metadata_srv.get_sorted_library_id_by_sequencing_time(eval_lib_ids)
        logger.info(recent_lib_id)
        self.assertEqual(recent_lib_id, mock_wts_meta_2.library_id)

    def test_get_sorted_library_id_by_sequencing_time_2in1(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_sorted_library_id_by_sequencing_time_2in1

        Scenario: what if 2 WTS tumors (of same Subject) in 1 sequencing!
        """
        # first tumor library
        mock_sqr: SequenceRun = factories.SequenceRunFactory()
        mock_wts_meta: LabMetadata = factories.WtsTumorLabMetadataFactory()
        mock_wts_lbr: LibraryRun = factories.WtsTumorLibraryRunFactory()

        # second tumor library
        mock_wts_meta_2: LabMetadata = factories.WtsTumorLabMetadataFactory2()
        mock_wts_lbr_2: LibraryRun = factories.WtsTumorLibraryRunFactory2()
        mock_wts_lbr_2.instrument_run_id = TestConstant.instrument_run_id.value
        mock_wts_lbr_2.run_id = TestConstant.run_id.value
        mock_wts_lbr_2.save()

        # assert our test database has proper mock state
        self.assertEqual(SequenceRun.objects.count(), 1)
        self.assertEqual(LibraryRun.objects.count(), 2)
        self.assertEqual(LabMetadata.objects.count(), 2)

        eval_lib_ids = [TestConstant.wts_library_id_tumor.value, mock_wts_meta_2.library_id]

        recent_lib_id = metadata_srv.get_sorted_library_id_by_sequencing_time(eval_lib_ids)
        logger.info(recent_lib_id)
        self.assertEqual(recent_lib_id, mock_wts_meta_2.library_id)
