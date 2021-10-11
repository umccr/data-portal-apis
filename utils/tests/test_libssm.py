from unittest import TestCase, skip

from utils import libssm


class LibSsmUnitTests(TestCase):
    pass


class LibSsmIntegrationTests(TestCase):

    @skip
    def test_get_secret(self):
        """
        python manage.py test utils.tests.test_libssm.LibSsmIntegrationTests.test_get_secret
        """

        key = "/iap/jwt-token"

        value = libssm.get_secret(key=key)

        self.assertIsNotNone(value)
        self.assertIsInstance(value, str)
        # print(value)
