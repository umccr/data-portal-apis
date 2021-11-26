import logging

from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase

from data_portal.models.labmetadata import LabMetadata
from data_portal.tests.factories import LabMetadataFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LabMetadataTestCase(TestCase):

    def setUp(self):
        lab_metadata: LabMetadata = LabMetadataFactory()
        logger.info('Create Object data')

    def test_get_labmetadata(self):

        logger.info("Test get specific library")

        try:
            get_complete_sequence = LabMetadata.objects.get(library_id='L2100001')
            self.assertEqual(get_complete_sequence.library_id, 'L2100001', 'Library result as expected.')
        except ObjectDoesNotExist:
            logger.info(f"Raised ObjectDoesNotExist which is not expected")
