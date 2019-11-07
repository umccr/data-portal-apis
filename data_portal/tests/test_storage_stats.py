import json

from django.test import TestCase
from django.urls import reverse

from data_portal.tests.factories import S3ObjectFactory, LIMSRowFactory, S3LIMSFactory


class StorageStatsTests(TestCase):
    """
    Test cases for storage stats api
    """
    def test_get_stats(self):
        """
        Test storage stats works as expected
        """
        # Create two linked and one not linked for both S3Object and LIMSRows
        # so we can differentiate the counts in the result
        s3_object_linked_1 = S3ObjectFactory()
        s3_object_linked_2 = S3ObjectFactory()

        s3_object_not_linked = S3ObjectFactory()

        lims_row_linked_1 = LIMSRowFactory()
        lims_row_linked_2 = LIMSRowFactory()

        lims_row_not_linked = LIMSRowFactory()

        s3_lims_1 = S3LIMSFactory(lims_row=lims_row_linked_1, s3_object=s3_object_linked_1)
        s3_lims_2 = S3LIMSFactory(lims_row=lims_row_linked_2, s3_object=s3_object_linked_2)

        response = self.client.get(reverse('storage-stats'))
        data = json.loads(response.content)
        self.assertEqual(3, data['total_s3']['value'])
        self.assertEqual(2, data['linked_s3']['value'])
        self.assertEqual(1, data['not_linked_s3']['value'])
        self.assertEqual(3, data['total_lims']['value'])
        self.assertEqual(2, data['linked_lims']['value'])
        self.assertEqual(1, data['not_linked_lims']['value'])
