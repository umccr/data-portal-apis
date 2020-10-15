from unittest import TestCase

from utils import libsqs


class LibSQSUnitTests(TestCase):

    def test_arn_to_name(self):
        """
        python -m unittest utils.tests.test_libsqs.LibSQSUnitTests.test_arn_to_name
        """
        q_name = libsqs.arn_to_name("arn:aws:sqs:ap-southeast-2:1234567890:data-portal-mock-queue.fifo")
        self.assertEqual(q_name, "data-portal-mock-queue.fifo")
