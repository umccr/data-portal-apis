import hashlib
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
        sha256 = hashlib.sha256()
        sha256.update("unique-hash-bucketstart/umccrise/pcgr/pcgr.html".encode("utf-8"))
        left = sha256.hexdigest()
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
        self.assertIsNotNone(found.unique_hash)

    def test_get_by_sash_results(self):
        """
        python manage.py test data_portal.models.tests.test_s3object.S3ObjectTests.test_get_by_sash_results
        """

        valid_keys = [
            "analysis/SBJ001/sash/12345/smlv_germline/file1.annotations.vcf.gz",
            "analysis/SBJ001/sash/12345/smlv_somatic/file2.filters_set.vcf.gz",
            "analysis/SBJ001/sash/12345/smlv_somatic/file3.pass.vcf.gz",
            "analysis/SBJ001/sash/12345/sv_somatic/file4.sv.prioritised.vcf.gz",
        ]

        invalid_keys = [
            "analysis/SBJ001/sash/12345/smlv_somatic/pcgr/file5.pass.vcf.gz"
        ]

        for key in valid_keys + invalid_keys:
            S3Object.objects.create(
                bucket='some-bucket',
                key =key,
                size = 1000,
                last_modified_date = now(),
                e_tag = 'etag',
            )

        sash_results = S3Object.objects.get_subject_sash_results(subject_id="SBJ001")
        list_sash_results = list(sash_results.values_list('key', flat=True))

        self.assertEqual(len(sash_results), 4, "4 valid key is expected")
        self.assertEqual(list_sash_results, valid_keys, 'only valid key is expected')
