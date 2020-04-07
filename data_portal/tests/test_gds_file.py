import logging

from django.db import IntegrityError
from django.test import TestCase

from data_portal.models import GDSFile
from data_portal.tests.factories import GDSFileFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GDSFileTests(TestCase):

    def test_save_gdsfile(self):
        gds_file: GDSFile = GDSFileFactory()
        logger.info(gds_file)
        logger.info(gds_file.inherited_acl)
        logger.info(gds_file.unique_hash)
        self.assertEqual(1, GDSFile.objects.count())
        self.assertEqual("Test.txt", GDSFile.objects.get(name=gds_file.name).name)

    def test_save_duplicate_gdsfile(self):
        gds_file: GDSFile = GDSFileFactory()
        logger.info(f"Created first GDSFile record. Its unique_hash: {gds_file.unique_hash}")
        self.assertEqual(1, GDSFile.objects.count())
        try:
            logger.info(f"Attempt to create another GDSFile with the same {gds_file.volume_name} and {gds_file.path}")
            gds_file_copycat: GDSFile = GDSFileFactory()
        except IntegrityError as e:
            logger.info(f"Raised IntegrityError: {e}")
        self.assertRaises(IntegrityError)
