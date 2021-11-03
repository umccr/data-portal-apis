import logging

from django.test import TestCase
from django.utils.timezone import now

from data_portal.fields import HashFieldHelper
from data_portal.models.s3object import S3Object
from data_portal.tests.factories import S3ObjectFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3ObjectTests(TestCase):

    def test_unique_hash(self):
        """
        python manage.py test data_portal.models.tests.test_s3object.S3ObjectTests.test_unique_hash
        """
        bucket = 'unique-hash-bucket'
        key = 'start/umccrise/pcgr/pcgr.html'

        # echo -n 'unique-hash-bucketstart/umccrise/pcgr/pcgr.html' | sha256sum
        left = '92f7602596b2952a0e695d29b444de3568968b2b7ed19a5fb0ecbf26e197681c'
        logger.info(f"Pre compute: (unique_hash={left})")

        s3_object = S3Object(bucket=bucket, key=key, size=0, last_modified_date=now(), e_tag='1234567890')
        s3_object.save()
        right = s3_object.unique_hash

        logger.info(f"DB save: (bucket={s3_object.bucket}, key={s3_object.key}, unique_hash={right})")

        self.assertEqual(left, right)

    def test_get_by_unique_hash(self):
        """
        python manage.py test data_portal.models.tests.test_s3object.S3ObjectTests.test_get_by_unique_hash
        """
        mock_s3_object: S3Object = S3ObjectFactory()

        logger.info(f"{mock_s3_object.bucket}, {mock_s3_object.key}, {mock_s3_object.unique_hash}")

        h = HashFieldHelper()
        h.add(mock_s3_object.bucket).add(mock_s3_object.key)
        unique_hash = h.calculate_hash()

        logger.info(f"unique_hash: {unique_hash}")

        found = S3Object.objects.filter(unique_hash__exact=unique_hash).get()

        self.assertIsNotNone(found)
        self.assertEqual(found.key, mock_s3_object.key)
        self.assertEqual(found.unique_hash, '4dbbc65280aa0cfb0f49bdd2f916cee72484a063b68366795aa96f822ab60e1e')
