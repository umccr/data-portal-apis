from unittest import TestCase, skip

from utils import libsm


class LibSmUnitTests(TestCase):
    pass


class LibSmIntegrationTests(TestCase):

    @skip
    def test_get_secret(self):
        """
        python manage.py test utils.tests.test_libsm.LibSmIntegrationTests.test_get_secret
        """

        secret_name = "IcaSecretsPortal"

        secret = libsm.get_secret(secret_name=secret_name)

        self.assertIsNotNone(secret)
        self.assertIsInstance(secret, str)
        # print(secret)
