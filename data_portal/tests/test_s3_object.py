import logging

from django.test import TestCase
from django.utils.timezone import now

from data_portal.models import S3Object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3ObjectTests(TestCase):

    def test_unique_hash(self):
        """
        Integration test for S3Object.unique_hash HashField data type
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
