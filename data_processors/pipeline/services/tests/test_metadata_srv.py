from data_portal.models import Workflow, LabMetadata
from data_portal.tests import factories
from data_portal.tests.factories import TestConstant
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.services import metadata_srv
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

    def test_get_subjects_from_runs(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_subjects_from_runs
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        mock_workflow = Workflow(
            sample_name=TestConstant.library_id_normal.value,
            type_name=WorkflowType.DRAGEN_WGS_QC.name,
        )
        subject_list = metadata_srv.get_subjects_from_runs([mock_workflow])
        self.assertEqual(subject_list[0], mock_meta.subject_id)

    def test_get_library_id_from_workflow(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_metadata_srv.MetadataSrvUnitTests.test_get_library_id_from_workflow
        """

        lib_id = metadata_srv.get_library_id_from_workflow(
            Workflow(
                sample_name=TestConstant.library_id_normal.value,
                type_name=WorkflowType.DRAGEN_WGS_QC.name,
            )
        )
        logger.info(lib_id)
        self.assertEqual(lib_id, TestConstant.library_id_normal.value)

        lib_id = metadata_srv.get_library_id_from_workflow(
            Workflow(
                sample_name=TestConstant.library_id_normal.value,
                type_name=WorkflowType.DRAGEN_TSO_CTDNA.name,
            )
        )
        logger.info(lib_id)
        self.assertEqual(lib_id, TestConstant.library_id_normal.value)

        lib_id = metadata_srv.get_library_id_from_workflow(
            Workflow(
                sample_name=TestConstant.sample_name_normal.value,
            )
        )
        logger.info(lib_id)
        self.assertEqual(lib_id, TestConstant.library_id_normal.value)