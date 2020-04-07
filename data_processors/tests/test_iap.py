import json
import logging

from django.test import TestCase

from data_portal.models import GDSFile
from data_portal.tests.factories import GDSFileFactory
from data_processors.exceptions import *
from data_processors.lambdas import iap

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class IAPLambdaTests(TestCase):

    def test_uploaded_gds_file_event(self):

        gds_file_message = {
            "id": "fil.8036f70c160549m1107500d7cf72d73p",
            "name": "IntegrationTest.txt",
            "volumeId": "vol.912zb524d44b434395b308d77g441333",
            "volumeName": "umccr-compliance-volume-name-prod",
            "tenantId": "AAdzLXVzLBBsXXXmb3JtOjEwWDGwNTM3OjBiYTU5YWUxLWZkYWUtNDNiYS1hM2I1LTRkMzY3TTQzOOJkBB",
            "subTenantId": "wid:f687447b-d13e-4464-a6b8-7167fc75742d",
            "path": "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt",
            "timeCreated": "2020-04-01T20:55:35.025Z",
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
                "Type": "String",
                "Value": "wid:f687447b-d13e-4464-a6b8-7167fc75742d"
            },
            "subscription-urn": {
                "Type": "String",
                "Value": "urn:ilmn:igp:us-east-1"
                         ":eXei3veelae8iNgae6aipoo7Hodeeh3ohshaeTai7iezib1oog5tha3Aixai3jeebi7shei5aePhej3eem"
                         ":subscription:sub.747 "
            },
            "contentversion": {
                "Type": "String",
                "Value": "V1"
            },
            "action": {
                "Type": "String",
                "Value": "uploaded"
            },
            "type": {
                "Type": "String",
                "Value": "gds.files"
            },
            "actiondate": {
                "Type": "String",
                "Value": "2020-04-01T20:55:36.7698333Z"
            },
            "producedby": {
                "Type": "String",
                "Value": "GenomicDataStoreService"
            },
            "contenttype": {
                "Type": "String",
                "Value": "application/json"
            }
        }

        sqs_event_message = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(gds_file_message),
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        iap.handler(sqs_event_message, None)

        volume = "umccr-compliance-volume-name-prod"
        path = "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt"
        qs = GDSFile.objects.filter(volume_name=volume, path=path)
        gds_file = qs.get()
        logger.info(f"Found GDSFile record from db: gds://{gds_file.volume_name}{gds_file.path}")
        self.assertEqual(1, qs.count())

    def test_unsupported_ens_event_type(self):

        ens_sqs_message_attributes = {
            "type": {
                "Type": "String",
                "Value": "tes.runs"
            }
        }

        sqs_event_message = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": "does_not_matter",
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        try:
            iap.handler(sqs_event_message, None)
        except UnsupportedIAPEventNotificationServiceType as e:
            logger.info(f"Raised: {e}")

        self.assertRaises(UnsupportedIAPEventNotificationServiceType)

    def test_deleted_gds_file_event(self):

        gds_file: GDSFile = GDSFileFactory()

        gds_file_message = {
            "id": gds_file.file_id,
            "name": gds_file.name,
            "volumeId": gds_file.volume_id,
            "volumeName": gds_file.volume_name,
            "tenantId": gds_file.tenant_id,
            "subTenantId": gds_file.sub_tenant_id,
            "path": gds_file.path,
            "timeCreated": gds_file.time_created,
            "createdBy": gds_file.created_by,
            "timeModified": gds_file.time_modified,
            "modifiedBy": gds_file.modified_by,
            "inheritedAcl": gds_file.inherited_acl,
            "urn": gds_file.urn,
            "sizeInBytes": gds_file.size_in_bytes,
            "isUploaded": gds_file.is_uploaded,
            "archiveStatus": gds_file.archive_status,
            "storageTier": gds_file.storage_tier
        }

        ens_sqs_message_attributes = {
            "action": {
                "Type": "String",
                "Value": "deleted"
            },
            "type": {
                "Type": "String",
                "Value": "gds.files"
            }
        }

        sqs_event_message = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(gds_file_message),
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        iap.handler(sqs_event_message, None)
        self.assertEqual(0, GDSFile.objects.count())
