from data_portal.models import GDSFile
from data_processors.pipeline.lambdas import sqs_iap_event, gds_search
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase
from utils import libjson


class GdsSearchLambdaUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(GdsSearchLambdaUnitTests, self).setUp()

    def test_search_gds_file_with_token(self):
        """
        python manage.py test data_processors.pipeline.tests.test_gds_search.GdsSearchLambdaUnitTests.test_search_gds_file_with_token
        """
        gds_file_message = {
            "id": "fil.8036f70c160549m1107500d7cf72d73p",
            "name": "IntegrationTest.txt",
            "volumeId": "vol.912zb524d44b434395b308d77g441333",
            "volumeName": "umccr-compliance-volume-name-prod",
            "tenantId": "AAdzLXVzLBBsXXXmb3JtOjEwWDGwNTM3OjBiYTU5YWUxLWZkYWUtNDNiYS1hM2I1LTRkMzY3TTQzOOJkBB",
            "subTenantId": "wid:f687447b-d13e-4464-a6b8-7167fc75742d",
            "path": "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt",
            "timeCreated": "2020-04-08T02:00:58.026467",
            "createdBy": "14c99f4f-8934-4af2-9df2-729e1b840f42",
            "timeModified": "2020-04-01T20:55:35.025Z",
            "modifiedBy": "14c99f4f-8934-4af2-9df2-729e1b840f42",
            "inheritedAcl": [
                "tid:ookohRahWee0ko1epoon3ej5tezeecu2thaec3AhsaSh3uqueeThasu0guTheeyeecheemoh9tu3neiGh0",
                "wid:cf5c71a5-85c9-4c60-971a-cd1426dbbd5e",
                "wid:58e3d90f-2570-4aeb-a606-bbde78eae677",
                "wid:f687447b-d13e-4464-a6b8-7167fc75742d"
            ],
            "urn": "urn:ilmn:iap:aps2"
                   ":AAdzLXVzLBBsXXXmb3JtOjEwWDGwNTM3OjBiYTU5YWUxLWZkYWUtNDNiYS1hM2I1LTRkMzY3TTQzOOJkBB:file:fil"
                   ".8036f70c160549m1107500d7cf72d73p#/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt",
            "sizeInBytes": 1000000000000000,
            "isUploaded": True,
            "archiveStatus": "None",
            "storageTier": "Standard"
        }
        ens_sqs_message_attributes = {
            "sub-tenant-id": {
                "stringValue": "uid:does-not-matter",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "subscription-urn": {
                "stringValue": "urn:does-not-matter",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "contentversion": {
                "stringValue": "V1",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "action": {
                "stringValue": "uploaded",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "actiondate": {
                "stringValue": "2020-04-08T02:00:59.9745859Z",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "type": {
                "stringValue": "gds.files",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "producedby": {
                "stringValue": "GenomicDataStoreService",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "contenttype": {
                "stringValue": "application/json",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            }
        }

        sqs_event_message = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": libjson.dumps(gds_file_message),
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        sqs_iap_event.handler(sqs_event_message, None)

        volume = "umccr-compliance-volume-name-prod"
        path = "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt"
        qs = GDSFile.objects.filter(volume_name=volume, path=path)
        gds_file = qs.get()
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found GDSFile record from db: gds://{gds_file.volume_name}{gds_file.path}")

        logger.info("Test match")
        search_payload = {
            'gds_volume_name': 'umccr-compliance-volume-name-prod',
            'tokens': ['Runs', 'A00130']
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertIsNotNone(results)

        logger.info("Test non-match")
        search_payload = {
            'gds_volume_name': 'umccr-compliance-volume-name-prod',
            'tokens': ['Runs', 'FooBar']
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertEqual(0, len(results['files']))


    def test_search_gds_file_with_regex(self):
        """
        python manage.py test data_processors.pipeline.tests.test_gds_search.GdsSearchLambdaUnitTests.test_search_gds_file_with_regex
        """
        gds_file_message = {
            "id": "fil.8036f70c160549m1107500d7cf72d73p",
            "name": "IntegrationTest.txt",
            "volumeId": "vol.912zb524d44b434395b308d77g441333",
            "volumeName": "umccr-compliance-volume-name-prod",
            "tenantId": "AAdzLXVzLBBsXXXmb3JtOjEwWDGwNTM3OjBiYTU5YWUxLWZkYWUtNDNiYS1hM2I1LTRkMzY3TTQzOOJkBB",
            "subTenantId": "wid:f687447b-d13e-4464-a6b8-7167fc75742d",
            "path": "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt",
            "timeCreated": "2020-04-08T02:00:58.026467",
            "createdBy": "14c99f4f-8934-4af2-9df2-729e1b840f42",
            "timeModified": "2020-04-01T20:55:35.025Z",
            "modifiedBy": "14c99f4f-8934-4af2-9df2-729e1b840f42",
            "inheritedAcl": [
                "tid:ookohRahWee0ko1epoon3ej5tezeecu2thaec3AhsaSh3uqueeThasu0guTheeyeecheemoh9tu3neiGh0",
                "wid:cf5c71a5-85c9-4c60-971a-cd1426dbbd5e",
                "wid:58e3d90f-2570-4aeb-a606-bbde78eae677",
                "wid:f687447b-d13e-4464-a6b8-7167fc75742d"
            ],
            "urn": "urn:ilmn:iap:aps2"
                   ":AAdzLXVzLBBsXXXmb3JtOjEwWDGwNTM3OjBiYTU5YWUxLWZkYWUtNDNiYS1hM2I1LTRkMzY3TTQzOOJkBB:file:fil"
                   ".8036f70c160549m1107500d7cf72d73p#/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt",
            "sizeInBytes": 1000000000000000,
            "isUploaded": True,
            "archiveStatus": "None",
            "storageTier": "Standard"
        }
        ens_sqs_message_attributes = {
            "sub-tenant-id": {
                "stringValue": "uid:does-not-matter",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "subscription-urn": {
                "stringValue": "urn:does-not-matter",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "contentversion": {
                "stringValue": "V1",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "action": {
                "stringValue": "uploaded",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "actiondate": {
                "stringValue": "2020-04-08T02:00:59.9745859Z",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "type": {
                "stringValue": "gds.files",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "producedby": {
                "stringValue": "GenomicDataStoreService",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "contenttype": {
                "stringValue": "application/json",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            }
        }

        sqs_event_message = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": libjson.dumps(gds_file_message),
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        sqs_iap_event.handler(sqs_event_message, None)

        volume = "umccr-compliance-volume-name-prod"
        path = "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt"
        qs = GDSFile.objects.filter(volume_name=volume, path=path)
        gds_file = qs.get()
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found GDSFile record from db: gds://{gds_file.volume_name}{gds_file.path}")

        logger.info("Test match")
        search_payload = {
            'gds_volume_name': 'umccr-compliance-volume-name-prod',
            'regex': '.+Runs.+Test.txt'
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertIsNotNone(results)

        logger.info("Test non-match")
        search_payload = {
            'gds_volume_name': 'umccr-compliance-volume-name-prod',
            'tokens': 'foobar'
        }
        results = gds_search.handler(search_payload, None)
        logger.info(f"Retrieved results: {results}")
        self.assertEqual(0, len(results['files']))

