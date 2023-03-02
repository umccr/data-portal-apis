import logging

from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase

from data_portal.models.labmetadata import LabMetadata
from data_portal.tests.factories import LabMetadataFactory, LibraryRunFactory, TumorLabMetadataFactory, TestConstant, \
    WtsTumorLabMetadataFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LabMetadataTestCase(TestCase):

    def setUp(self):
        LabMetadataFactory()
        logger.info('Create Object data')

    def test_get_labmetadata(self):

        logger.info("Test get specific library")

        try:
            get_complete_sequence = LabMetadata.objects.get(library_id='L2100001')
            self.assertEqual(get_complete_sequence.library_id, 'L2100001', 'Library result as expected.')
        except ObjectDoesNotExist:
            logger.info(f"Raised ObjectDoesNotExist which is not expected")

    def test_get_by_keyword_not_sequenced(self):
        # python manage.py test data_portal.models.tests.test_labmetadata.LabMetadataTestCase.test_get_by_keyword_not_requenced

        logger.info("Test exclusion of metadata for unsequenced libraries")
        TumorLabMetadataFactory()  # does not have a LibraryRun entry, i.e. not sequenced (yet) (tumor sample)
        LibraryRunFactory()  # LibraryRun entry for metadata created with LabMetadataFactory() (normal sample)

        # The normal library has a LibraryRun entry, i,e. has been sequenced, therefore
        # we expect to find it in a full metadata search
        lib = LabMetadata.objects.get_by_keyword(library_id=TestConstant.library_id_normal.value)
        self.assertEqual(len(lib), 1, 'Expect metadata for normal library')
        # and when excluding unsequenced libraries
        lib = LabMetadata.objects.get_by_keyword(library_id=TestConstant.library_id_normal.value, sequenced=True)
        self.assertEqual(len(lib), 1, 'Expect metadata for normal library')

        # The tumor library has no LibraryRun entry, i,e. has NOT been sequenced, therefore
        # we expect to find it in a full metadata search
        lib = LabMetadata.objects.get_by_keyword(library_id=TestConstant.library_id_tumor.value)
        self.assertEqual(len(lib), 1, 'Expect matadata for tumor library')
        # and NOT when excluding unsequenced libraries
        lib = LabMetadata.objects.get_by_keyword(library_id=TestConstant.library_id_tumor.value, sequenced=True)
        self.assertEqual(len(lib), 0, 'Did NOT expect metadat for tumor library (not sequenced yet)')

    def test_get_by_keyword_in_not_sequenced(self):
        # python manage.py test data_portal.models.tests.test_labmetadata.LabMetadataTestCase.test_get_by_keyword_in_not_requenced

        logger.info("Test exclusion of metadata for unsequenced libraries")
        TumorLabMetadataFactory()  # does not have a LibraryRun entry, i.e. not sequenced (yet) (tumor sample)
        WtsTumorLabMetadataFactory()  # does not have a LibraryRun entry, i.e. not sequenced (yet) (tumor sample)
        LibraryRunFactory()  # LibraryRun entry for metadata created with LabMetadataFactory() (normal sample)

        # we expect to find both record in a full metadata search
        lib = LabMetadata.objects.get_by_keyword_in(libraries=[TestConstant.library_id_normal.value, TestConstant.wts_library_id_tumor.value])
        self.assertEqual(len(lib), 2, 'Expect metadata for normal library')
        # but only the normal sample when excluding unsequenced libraries
        lib = LabMetadata.objects.get_by_keyword_in(libraries=[TestConstant.library_id_normal.value, TestConstant.wts_library_id_tumor.value], sequenced=True)
        self.assertEqual(len(lib), 1, 'Expect metadata for normal library')

        # The tumor libraries have no LibraryRun entry, i,e. have NOT been sequenced, therefore
        # we expect to find both in a full metadata search
        lib = LabMetadata.objects.get_by_keyword_in(libraries=[TestConstant.library_id_tumor.value, TestConstant.wts_library_id_tumor.value])
        self.assertEqual(len(lib), 2, 'Expect matadata for tumor library')
        # but none when excluding unsequenced libraries
        lib = LabMetadata.objects.get_by_keyword_in(libraries=[TestConstant.library_id_tumor.value, TestConstant.wts_library_id_tumor.value], sequenced=True)
        self.assertEqual(len(lib), 0, 'Did NOT expect metadat for tumor library (not sequenced yet)')
