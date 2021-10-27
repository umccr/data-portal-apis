from django.core.exceptions import ObjectDoesNotExist

from data_portal.models.gdsfile import GDSFile
from data_portal.models.report import Report
from data_portal.tests import factories
from data_portal.tests.factories import GDSFileFactory
from data_processors.gds.lambdas import gds_event
from data_processors.gds.tests.case import logger, GDSEventUnitTestCase
from utils import libjson


def _make_mock_sqs_message():
    gds_file: GDSFile = GDSFileFactory()
    gds_file.path = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"

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
        "actiondate": {
            "stringValue": "2020-04-08T02:00:59.9745859Z",
        },
        "action": {
            "stringValue": "uploaded",
        },
        "type": {
            "stringValue": "gds.files",
        },
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

    return sqs_event_message


class GDSEventUnitTests(GDSEventUnitTestCase):

    def test_uploaded_gds_file_event(self):
        """
        python manage.py test data_processors.gds.tests.test_gds_event.GDSEventUnitTests.test_uploaded_gds_file_event
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

        gds_event.handler(sqs_event_message, None)

        volume = "umccr-compliance-volume-name-prod"
        path = "/Runs/200401_A00130_0134_BHT5N3DMXX/IntegrationTest.txt"
        qs = GDSFile.objects.filter(volume_name=volume, path=path)
        gds_file = qs.get()
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found GDSFile record from db: gds://{gds_file.volume_name}{gds_file.path}")

    def test_deleted_gds_file_event(self):
        """
        python manage.py test data_processors.gds.tests.test_gds_event.GDSEventUnitTests.test_deleted_gds_file_event
        """

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
            "actiondate": {
                "stringValue": "2020-04-08T02:00:59.9745859Z",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "action": {
                "stringValue": "deleted",
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

        gds_event.handler(sqs_event_message, None)
        self.assertEqual(0, GDSFile.objects.count())

    def test_delete_non_existent_gds_file(self):
        """
        python manage.py test data_processors.gds.tests.test_gds_event.GDSEventUnitTests.test_delete_non_existent_gds_file
        """

        gds_file_message = {
            "volumeName": "test",
            "path": "/this/does/not/exist/in/db/gds_file.path",
        }

        ens_sqs_message_attributes = {
            "actiondate": {
                "stringValue": "2020-04-08T02:00:59.9745859Z",
            },
            "action": {
                "stringValue": "deleted",
            },
            "type": {
                "stringValue": "gds.files",
            },
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

        gds_event.handler(sqs_event_message, None)
        self.assertRaises(ObjectDoesNotExist)

    def test_handler_report_queue(self):
        """
        python manage.py test data_processors.gds.tests.test_gds_event.GDSEventUnitTests.test_handler_report_queue
        """
        self.verify_local()
        results = gds_event.handler(_make_mock_sqs_message(), None)
        logger.info(libjson.dumps(results))
        self.assertEqual(results['created_or_updated_count'], 1)

    def test_delete_gds_file_linked_with_report(self):
        """
        python manage.py test data_processors.gds.tests.test_gds_event.GDSEventUnitTests.test_delete_gds_file_linked_with_report
        """

        gds_file: GDSFile = GDSFileFactory()
        gds_file.path = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"
        gds_file.save()

        mock_report: Report = factories.FusionCallerMetricsReportFactory()
        mock_report.gds_file_id = gds_file.id
        mock_report.save()

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
            "actiondate": {
                "stringValue": "2020-04-08T02:00:59.9745859Z",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "action": {
                "stringValue": "deleted",
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

        gds_event.handler(sqs_event_message, None)

        report_in_db = Report.objects.get(id__exact=mock_report.id)
        self.assertIsNotNone(report_in_db.gds_file_id)

    def test_parse_raw_gds_event_records(self):
        """
        python manage.py test data_processors.gds.tests.test_gds_event.GDSEventUnitTests.test_parse_raw_gds_event_records
        """
        event_records_dict = gds_event.parse_raw_gds_event_records(_make_mock_sqs_message()['Records'])
        self.assertEqual(len(event_records_dict['gds_event_records']), 1)
        self.assertEqual(len(event_records_dict['report_event_records']), 1)
