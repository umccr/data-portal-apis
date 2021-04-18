import logging

from django.test import TestCase
from mockito import unstub

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ReportUnitTestCase(TestCase):

    def setUp(self) -> None:
        # some code construct that share across all test cases under reports package
        # pass for now
        pass

    def tearDown(self) -> None:
        # undo any construct done from setUp
        unstub()


class ReportIntegrationTestCase(TestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
