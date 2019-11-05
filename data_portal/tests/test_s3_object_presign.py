import os
from unittest import TestCase

import boto3
from django.urls import reverse
from moto import mock_s3
from rest_framework import status
from rest_framework.test import APIClient


@mock_s3
class S3ObjectPreSignTests(TestCase):
    def setUp(self) -> None:
        self.client = APIClient()
        # Important for testing, ensuring we are not mutating actual infrastructure
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'

        boto3.DEFAULT_SESSION = None

        self.bucket_name = 'some-bucket'
        self.conn = boto3.resource('s3', region_name='us-east-1')
        self.bucket = self.conn.create_bucket(Bucket=self.bucket_name)

    def test_invalid_parameters(self):
        """
        Test calling the API with invalid parameters and we should get an error
        """
        response = self.client.get(reverse('file-signed-url'))
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_valid_parameters(self):
        """
        Test calling the API with valid parameters and we should get a success response
        """

        key = 'some-file.csv'
        self.bucket.put_object(Bucket=self.bucket_name, Key=key, Body="")

        response = self.client.get(reverse('file-signed-url') + "?bucket=%s&key=%s" % (self.bucket_name, key))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
