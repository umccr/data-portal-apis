from django.test import TestCase
from django.utils.timezone import now

from data_portal.models import S3Object
from data_portal.tests import factories
from data_processors.s3 import lambdas


class ReportsTests(TestCase):

    def setUp(self) -> None:
        super(ReportsTests, self).setUp()
        self.hrd_report = factories.HRDReportFactory()
        self.purple_report = factories.HRDReportFactory()

    def test_sqs_s3_event_processor(self) -> None:
        """
        python manage.py test data_processors.reports.tests.ReportsTests.test_sqs_s3_event_processor

        Test whether the report can be consumed from the SQS queue as expected
        """
        # jq . SBJ00670__SBJ00670_MDX210005_L2100047_rerun-hrdetect.json
        # [
        #   {
        #     "sample": "SBJ00670_MDX210005_L2100047_rerun",
        #     "Probability": 0.034,
        #     "intercept": -3.364,
        #     "del.mh.prop": -0.757,
        #     "SNV3": 2.571,
        #     "SV3": -0.877,
        #     "SV5": -1.105,
        #     "hrdloh_index": 0.096,
        #     "SNV8": 0.079
        #   }
        # ]
        # Compose test data

        print('-' * 32)
        print('\t\t >>> ', self.hrd_report)
        print('-' * 32)

        bucket_name = 'some-bucket'
        report_to_deserialize = 'cancer_report_tables/json/hrd/SBJ00670__SBJ00670_MDX210005_L2100047_rerun-hrdetect.json.gz'

        # Create an S3Object first with the key to be deleted
        s3_object = S3Object(bucket=bucket_name, key=report_to_deserialize,
                             size=0, last_modified_date=now(), e_tag='')
        s3_object.save()

        s3_event_message = {
            "Records": [
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectRemoved",
                    "s3": {
                        "bucket": {
                            "name": bucket_name,
                        },
                        "object": {
                            "key": report_to_deserialize,
                            "size": 1,
                            "eTag": "object eTag",
                        }
                    }
                },
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated",
                    "s3": {
                        "bucket": {
                            "name": bucket_name
                        },
                        "object": {
                            "key": report_to_deserialize,
                            "size": 1,
                            "eTag": "object eTag",
                        }
                    }
                }
            ]
        }

        sqs_event = {
            "Records": [
                {
                    "body": str(s3_event_message),
                }
            ]
        }

        _ = lambdas.handler(sqs_event, None)
