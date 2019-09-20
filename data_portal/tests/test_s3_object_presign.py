from unittest import TestCase

import boto3
from django.urls import reverse
from moto import mock_s3
from rest_framework import status
from rest_framework.test import APIClient


class S3ObjectPreSignTests(TestCase):
    def setUp(self) -> None:
        self.client = APIClient()

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
        with mock_s3():
            bucket_name = 'some-bucket'
            key = 'some-file.csv'
            conn = boto3.resource('s3', region_name='us-east-1')
            bucket = conn.create_bucket(Bucket=bucket_name)
            bucket.put_object(Bucket=bucket, Key=key, Body="")

            response = self.client.get(reverse('file-signed-url') + "?bucket=%s&key=%s" % (bucket_name, key))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
