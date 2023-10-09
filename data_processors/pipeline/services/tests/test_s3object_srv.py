from django.utils.timezone import now

from data_portal.models import S3Object
from data_portal.tests.factories import TestConstant
from data_processors.pipeline.services import s3object_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


def generate_mock_data():
    mock_portal_run_id = TestConstant.portal_run_id.value

    mock_bam = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/PRJ421001.md.bam",
        size=1000,
        last_modified_date=now(),
        e_tag="abcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/PRJ421001.md.bam.bai",
        size=1000,
        last_modified_date=now(),
        e_tag="abcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/2222.bam",
        size=1000,
        last_modified_date=now(),
        e_tag="abcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/",
        size=1100,
        last_modified_date=now(),
        e_tag="bbcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/PRJ421001.vcf.gz",
        size=1001,
        last_modified_date=now(),
        e_tag="cccdefghi123456"
    )

    return mock_bam


class S3ObjectSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(S3ObjectSrvUnitTests, self).setUp()

    def test_get_s3_files_for_path_tokens(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_s3object_srv.S3ObjectSrvUnitTests.test_get_s3_files_for_path_tokens
        """

        mock_bam = generate_mock_data()

        results = s3object_srv.get_s3_files_for_path_tokens(path_tokens=[
            TestConstant.portal_run_id.value,
            ".bam"
        ])

        logger.info(results)
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], f"s3://{mock_bam.bucket}/{mock_bam.key}")

    def test_get_s3_files_for_regex(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_s3object_srv.S3ObjectSrvUnitTests.test_get_s3_files_for_regex
        """
        generate_mock_data()

        results = s3object_srv.get_s3_files_for_regex(pattern=f"{TestConstant.portal_run_id.value}/L4200001/PRJ421001/$")

        logger.info(results)
        self.assertEqual(len(results), 1)
        self.assertTrue(str(results[0]).startswith("s3://"))
