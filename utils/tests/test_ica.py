import logging
import os
from unittest import TestCase

from mockito import unstub

from utils.ica import GDSFilesEventType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IcaUnitTests(TestCase):

    def setUp(self) -> None:
        super(IcaUnitTests, self).setUp()
        os.environ['ICA_BASE_URL'] = "http://localhost"
        os.environ['ICA_ACCESS_TOKEN'] = "mock"

    def tearDown(self) -> None:
        del os.environ['ICA_BASE_URL']
        del os.environ['ICA_ACCESS_TOKEN']
        unstub()

    def verify_local(self):
        logger.info(f"ICA_BASE_URL={os.getenv('ICA_BASE_URL')}")
        assert os.environ['ICA_BASE_URL'] == "http://localhost"
        assert os.environ['ICA_ACCESS_TOKEN'] == "mock"
        self.assertEqual(os.environ['ICA_BASE_URL'], "http://localhost")

    def test_gds_files_event_type(self):
        """
        python -m unittest utils.tests.test_ica.IcaUnitTests.test_gds_files_event_type
        """
        event_type = GDSFilesEventType.from_value("deleted")
        self.assertEqual(event_type, GDSFilesEventType.DELETED)

        event_type = GDSFilesEventType.from_value("uploaded")
        self.assertEqual(event_type, GDSFilesEventType.UPLOADED)

        event_type = GDSFilesEventType.from_value("archived")
        self.assertEqual(event_type, GDSFilesEventType.ARCHIVED)

        event_type = GDSFilesEventType.from_value("unarchived")
        self.assertEqual(event_type, GDSFilesEventType.UNARCHIVED)

        try:
            _ = GDSFilesEventType.from_value("unknown")
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)


class IcaIntegrationTests(TestCase):
    pass
