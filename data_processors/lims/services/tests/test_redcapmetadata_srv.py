from data_portal.tests import factories
from data_portal.tests.factories import TestConstant
from data_processors.lims.services import redcapmetadata_srv
from data_processors.lims.tests.case import logger, LimsUnitTestCase


class RedcapMetadataSrvUnitTests(LimsUnitTestCase):

    def test_get_metadata_by_subject_id(self):
        """
        python manage.py test data_processors.lims.services.tests.test_redcapmetadata_srv.RedcapMetadataSrvUnitTests.test_get_metadata_by_library_id
        """
        #mock_meta: LabMetadata = factories.LabMetadataFactory()
        meta = redcapmetadata_srv.retrieve_metadata(TestConstant.sample_name_normal.value)
        self.assertIsNotNone(meta)

    def test_get_metadata_by_library_id_not_found(self):
        """
        python manage.py test data_processors.lims.services.tests.test_redcapmetadata_srv.RedcapMetadataSrvUnitTests.test_get_metadata_by_library_id_not_found
        """
        meta = redcapmetadata_srv.retrieve_metadata()
        self.assertIsNone(meta)
