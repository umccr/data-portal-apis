from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase
from django.utils.timezone import now

from data_portal.models import Report, S3Object
from data_processors.s3 import lambdas


class ReportsTests(TestCase):

    def test_sqs_s3_event_processor(self) -> None:
        """
        Test whether the report can be consumed from the SQS queue as expected
        """

        # Compose test data
        report = Report(
            subject_id='SBJ00001',
            sample_id='PRJ00001',
            library_id='L0000001',
            hrd_hrdetect='foo',
            hrd_results_hrdetect='bar',
            hrd_chord='baz',
            hrd_chord2='moo',
            hrd_results_chord='moo1',
            hrd_results_chord2='moo2'
        )
        report.save()

        bucket_name = 'some-bucket'
        report_to_deserialize = 'hrd.json.gz'

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
                            "key": f"{lims_row.subject_id}/{lims_row.sample_id}/test.json",
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

        results = lambdas.handler(sqs_event, None)

        self.assertEqual(results['removed_count'], 1)
        # We should expect the existing association removed as well
        self.assertEqual(results['s3_lims_removed_count'], 1)

        self.assertEqual(results['created_count'], 1)
        # We should expect the new association created as well
        self.assertEqual(results['s3_lims_created_count'], 1)
        self.assertEqual(results['unsupported_count'], 0)