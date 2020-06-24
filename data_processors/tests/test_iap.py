import json
import logging
import os

from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase
from mockito import when, unstub, mock

from data_portal.models import GDSFile, SequenceRun, Workflow
from data_portal.tests.factories import GDSFileFactory, WorkflowFactory
from data_processors.exceptions import *
from data_processors.lambdas import iap
from data_processors.pipeline.dto import FastQ
from data_processors.pipeline.factory import FastQBuilder
from data_processors.tests import _rand, _uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class IAPLambdaTests(TestCase):

    def setUp(self) -> None:
        os.environ['IAP_BASE_URL'] = "http://localhost"
        os.environ['IAP_AUTH_TOKEN'] = "mock"
        os.environ['IAP_WES_WORKFLOW_ID'] = WorkflowFactory.wfl_id
        os.environ['IAP_WES_WORKFLOW_VERSION_NAME'] = WorkflowFactory.version

    def tearDown(self) -> None:
        del os.environ['IAP_BASE_URL']
        del os.environ['IAP_AUTH_TOKEN']
        del os.environ['IAP_WES_WORKFLOW_ID']
        del os.environ['IAP_WES_WORKFLOW_VERSION_NAME']
        unstub()

    def test_uploaded_gds_file_event(self):

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
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found GDSFile record from db: gds://{gds_file.volume_name}{gds_file.path}")

    def test_unsupported_ens_event_type(self):

        ens_sqs_message_attributes = {
            "type": {
                "stringValue": "tes.runs",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
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
                    "body": json.dumps(gds_file_message),
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        iap.handler(sqs_event_message, None)
        self.assertEqual(0, GDSFile.objects.count())

    def test_delete_non_existent_gds_file(self):
        gds_file_message = {
            "volumeName": "test",
            "path": "/this/does/not/exist/in/db/gds_file.path",
        }

        ens_sqs_message_attributes = {
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
                    "body": json.dumps(gds_file_message),
                    "messageAttributes": ens_sqs_message_attributes
                }
            ]
        }

        iap.handler(sqs_event_message, None)
        self.assertRaises(ObjectDoesNotExist)

    def test_sequence_run_event(self):

        mock_run_id = "r.ACGxTAC8mGCtAcgTmITyDA"
        mock_instrument_run_id = "200508_A01052_0001_AC5GT7ACGT"
        mock_date_modified = "2020-05-09T22:17:03.1015272Z"
        mock_status = "Complete"

        sequence_run_message = {
            "gdsFolderPath": f"/Runs/{mock_instrument_run_id}_{mock_run_id}",
            "gdsVolumeName": "bssh.acgtacgt498038ed99fa94fe79523959",
            "reagentBarcode": "NV9999999-ACGTA",
            "v1pre3Id": "666666",
            "dateModified": mock_date_modified,
            "acl": [
                "wid:e4730533-d752-3601-b4b7-8d4d2f6373de"
            ],
            "flowcellBarcode": "BARCODEEE",
            "sampleSheetName": "MockSampleSheet.csv",
            "apiUrl": f"https://api.aps2.sh.basespace.illumina.com/v2/runs/{mock_run_id}",
            "name": mock_instrument_run_id,
            "id": mock_run_id,
            "instrumentRunId": mock_instrument_run_id,
            "status": mock_status
        }

        ens_sqs_message_attributes = {
            "action": {
                "stringValue": "statuschanged",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "actiondate": {
                "stringValue": "2020-05-09T22:17:10.815Z",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "type": {
                "stringValue": "bssh.runs",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "producedby": {
                "stringValue": "BaseSpaceSequenceHub",
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
                    "body": json.dumps(sequence_run_message),
                    "messageAttributes": ens_sqs_message_attributes,
                    "attributes": {
                        "ApproximateReceiveCount": "3",
                        "SentTimestamp": "1589509337523",
                        "SenderId": "ACTGAGCTI2IGZA4XHGYYY:sender-sender",
                        "ApproximateFirstReceiveTimestamp": "1589509337535"
                    },
                    "eventSourceARN": "arn:aws:sqs:ap-southeast-2:843407916570:my-queue",
                }
            ]
        }

        # Comment the following mock to actually send slack message for this test case. i.e.
        # export SLACK_CHANNEL=#arteria-dev
        # python manage.py test data_processors.tests.test_iap.IAPLambdaTests.test_sequence_run_event
        #
        os.environ['SLACK_CHANNEL'] = "#mock"
        mock_response = mock(iap.srv.libslack.http.client.HTTPResponse)
        mock_response.status = 200
        when(iap.srv.libslack.libssm).get_ssm_param(...).thenReturn("mock_webhook_id_123")
        when(iap.srv.libslack.http.client.HTTPSConnection).request(...).thenReturn('ok')
        when(iap.srv.libslack.http.client.HTTPSConnection).getresponse(...).thenReturn(mock_response)

        iap.handler(sqs_event_message, None)

        qs = SequenceRun.objects.filter(run_id=mock_run_id)
        sqr = qs.get()
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found SequenceRun record from db: {sqr}")

        # assert bcl convert workflow launch success and save workflow runs in portal db
        success_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, success_bcl_convert_workflow_runs.count())

    def test_wes_runs_historyevents_germline(self):
        """
        Scenario:
        In order to kick off germline workflow, bcl convert must be completed at least.
        And also the related Sequence Run, quite implicitly.
        So, need to mock these two states and put them into test db first.
        Then call iap lambda with the mock wes event message.
        """
        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_fastq: FastQ = FastQ()
        mock_fastq.volume_name = f"{mock_bcl_workflow.wfr_id}"
        mock_fastq.path = f"/bclConversion_launch/try-1/out-dir-bclConvert"
        mock_fastq.gds_path = f"gds://{mock_fastq.volume_name}{mock_fastq.path}"
        mock_fastq.fastq_map = {
            'SAMPLE_ACGT1': {
                'fastq_list': ['SAMPLE_ACGT1_S1_L001_R1_001.fastq.gz', 'SAMPLE_ACGT1_S1_L001_R2_001.fastq.gz'],
                'tags': ['SBJ00001'],
            },
            'SAMPLE_ACGT2': {
                'fastq_list': ['SAMPLE_ACGT2_S2_L001_R1_001.fastq.gz', 'SAMPLE_ACGT2_S2_L001_R2_001.fastq.gz'],
                'tags': ['SBJ00001'],
            },
        }
        when(FastQBuilder).build().thenReturn(mock_fastq)

        workflow_run_message = {
            "Timestamp": "2020-05-18T06:47:46.146Z",
            "EventType": "RunSucceeded",
            "EventDetails": {},
            "WorkflowRun": {
                "TenantId": f"{_rand(82)}",
                "Status": "Succeeded",
                "TimeModified": "2020-05-18T06:47:19.20065",
                "Acl": [
                    f"tid:{_rand(82)}",
                    f"wid:{_uuid()}"
                ],
                "WorkflowVersion": {
                    "Id": f"{mock_bcl_workflow.wfv_id}",
                    "Language": {
                        "Name": "CWL",
                        "Version": "1.1"
                    },
                    "Version": "v1",
                    "Status": "Active",
                    "TimeCreated": "2020-05-18T06:26:05.070575",
                    "TimeModified": "2020-05-18T06:27:32.787349",
                    "TenantId": f"{_rand(82)}",
                    "Description": "Uses sambamba slice and samtools to extract bam region of interest.",
                    "CreatedBy": f"{_uuid()}",
                    "Href": f"https://aps2.platform.illumina.com/v1/workflows/"
                            f"{WorkflowFactory.wfl_id}/versions/{WorkflowFactory.version}",
                    "Acl": [
                        f"tid:{_rand(82)}",
                        f"wid:{_uuid()}"
                    ],
                    "ModifiedBy": f"{_uuid()}"
                },
                "Id": f"{mock_bcl_workflow.wfr_id}",
                "TimeCreated": "2020-05-18T06:27:32.662549",
                "StatusSummary": "",
                "TimeStarted": "2020-05-18T06:27:32.956904",
                "CreatedBy": f"{_uuid()}",
                "Href": f"https://aps2.platform.illumina.com/v1/workflows/runs/{mock_bcl_workflow.wfr_id}",
                "TimeStopped": "2020-05-18T06:47:46.146+00:00",
                "ModifiedBy": f"{_uuid()}"
            }
        }

        ens_sqs_message_attributes = {
            "action": {
                "stringValue": "succeeded",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "actiondate": {
                "stringValue": "2020-05-09T22:17:10.815Z",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "type": {
                "stringValue": "wes.runs.historyevents",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            },
            "producedby": {
                "stringValue": "WorkflowExecutionService",
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
                    "body": json.dumps(workflow_run_message),
                    "messageAttributes": ens_sqs_message_attributes,
                    "attributes": {
                        "ApproximateReceiveCount": "3",
                        "SentTimestamp": "1589509337523",
                        "SenderId": "ACTGAGCTI2IGZA4XHGYYY:sender-sender",
                        "ApproximateFirstReceiveTimestamp": "1589509337535"
                    },
                    "eventSourceARN": "arn:aws:sqs:ap-southeast-2:843407916570:my-queue",
                }
            ]
        }

        iap.handler(sqs_event_message, None)

        # assert germline workflow launch success and save workflow runs in portal db
        success_germline_workflow_runs = Workflow.objects.all()
        self.assertEqual(3, success_germline_workflow_runs.count())
