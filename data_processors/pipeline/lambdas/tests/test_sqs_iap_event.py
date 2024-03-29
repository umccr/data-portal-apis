import uuid
from datetime import datetime

from django.utils.timezone import make_aware
from libica.openapi import libwes
from libumccr import libslack, libjson
from libumccr.aws import libssm
from mockito import when, verify

from data_portal.models.batchrun import BatchRun
from data_portal.models.labmetadata import LabMetadata, LabMetadataType, LabMetadataAssay, LabMetadataWorkflow
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType, WorkflowRunEventType, \
    SequenceRuleError
from data_processors.pipeline.lambdas import sqs_iap_event
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests import _rand, _uuid
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase
from data_processors.pipeline.tools import liborca


def _mock_bcl_convert_output():
    return {
        'main/fastq_list_rows': [
            {
                "lane": 4,
                "read_1": {
                    "basename": "PTC_EXPn200908LL_L2000001_S1_L004_R1_001.fastq.gz",
                    "class": "File",
                    "http://commonwl.org/cwltool#generation": 0,
                    "location": "gds://fastqvol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_EXPn200908LL_L2000001_S1_L004_R1_001.fastq.gz",
                    "nameext": ".gz",
                    "nameroot": "PTC_EXPn200908LL_L2000001_S1_L004_R1_001.fastq",
                    "size": 26376114564
                },
                "read_2": {
                    "basename": "PTC_EXPn200908LL_L2000001_S1_L004_R2_001.fastq.gz",
                    "class": "File",
                    "http://commonwl.org/cwltool#generation": 0,
                    "location": "gds://fastqvol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_EXPn200908LL_L2000001_S1_L004_R2_001.fastq.gz",
                    "nameext": ".gz",
                    "nameroot": "PTC_EXPn200908LL_L2000001_S1_L004_R2_001.fastq",
                    "size": 25995547898
                },
                "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                "rgsm": "PTC_EXPn200908LL_L2000001",
                "rglb": "L2000001"
            },
            {
                "lane": 3,
                "read_1": {
                    "basename": "PTC_EXPn200908LL_L2000001_topup_S1_L004_R1_001.fastq.gz",
                    "class": "File",
                    "http://commonwl.org/cwltool#generation": 0,
                    "location": "gds://fastqvol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_EXPn200908LL_L2000001_topup_S1_L004_R1_001.fastq.gz",
                    "nameext": ".gz",
                    "nameroot": "PTC_EXPn200908LL_L2000001_topup_S1_L004_R1_001.fastq",
                    "size": 26376114564
                },
                "read_2": {
                    "basename": "PTC_EXPn200908LL_L2000001_topup_S1_L004_R2_001.fastq.gz",
                    "class": "File",
                    "http://commonwl.org/cwltool#generation": 0,
                    "location": "gds://fastqvol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_EXPn200908LL_L2000001_topup_S1_L004_R2_001.fastq.gz",
                    "nameext": ".gz",
                    "nameroot": "PTC_EXPn200908LL_L2000001_topup_S1_L004_R2_001.fastq",
                    "size": 25995547898
                },
                "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                "rgsm": "PTC_EXPn200908LL_L2000001_topup",
                "rglb": "L2000001"
            }
        ]
    }


def _sqs_wes_event_message(wfv_id, wfr_id, workflow_status: WorkflowStatus = WorkflowStatus.SUCCEEDED):
    event_details = {}
    if workflow_status == WorkflowStatus.FAILED:
        event_details = {
            "Error": "Workflow.Failed",
            "Cause": "Run Failed. Reason: task: [samplesheetSplit_launch] details: [Failed to submit TES Task. Reason "
                     "[(500)\nReason: Internal Server Error\nHTTP response headers: HTTPHeaderDict({'Date': 'Mon, 29 "
                     "Jun 2020 07:37:16 GMT', 'Content-Type': 'application/json', 'Server': 'Kestrel', "
                     "'Transfer-Encoding': 'chunked'})\nHTTP response body: {\"code\":\"\",\"message\":\"We had an "
                     "unexpected issue.  Please try your request again.  The issue has been logged and we are looking "
                     "into it.\"}\n]]"
        }

    elif workflow_status == WorkflowStatus.ABORTED:
        event_details = {
            "Error": "Workflow.Engine",
            "Cause": "Internal error while starting the run: DomainException: \n"
                     "Launch workflow via. airflow execution service failed. ErrorCode: 1\n",
        }

    workflow_run_message = {
        "Timestamp": "2020-05-18T06:47:46.146Z",
        "EventType": f"Run{workflow_status.value}",
        "EventDetails": event_details,
        "WorkflowRun": {
            "TenantId": f"{_rand(82)}",
            "Status": f"{workflow_status.value}",
            "TimeModified": "2020-05-18T06:47:19.20065",
            "Acl": [
                f"tid:{_rand(82)}",
                f"wid:{_uuid()}"
            ],
            "WorkflowVersion": {
                "Id": f"{wfv_id}",
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
            "Id": f"{wfr_id}",
            "TimeCreated": "2020-05-18T06:27:32.662549",
            "StatusSummary": "",
            "TimeStarted": "2020-05-18T06:27:32.956904",
            "CreatedBy": f"{_uuid()}",
            "Href": f"https://aps2.platform.illumina.com/v1/workflows/runs/{wfr_id}",
            "TimeStopped": "2020-05-18T06:47:46.146+00:00",
            "ModifiedBy": f"{_uuid()}"
        }
    }

    ens_sqs_message_attributes = {
        "action": {
            "stringValue": "updated",
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": "String"
        },
        "actionDate": {
            "stringValue": "2020-05-09T22:17:10.815Z",
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": "String"
        },
        "type": {
            "stringValue": "wes.runs",
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": "String"
        },
        "producedBy": {
            "stringValue": "WorkflowExecutionService",
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": "String"
        },
        "contentType": {
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
                "eventSourceARN": "arn:aws:sqs:ap-southeast-2:843407916570:my-queue",
                "awsRegion": "ap-southeast-2",
                "body": libjson.dumps(workflow_run_message),
                "messageAttributes": ens_sqs_message_attributes,
                "attributes": {
                    "ApproximateReceiveCount": "3",
                    "SentTimestamp": "1589509337523",
                    "SenderId": "ACTGAGCTI2IGZA4XHGYYY:sender-sender",
                    "ApproximateFirstReceiveTimestamp": "1589509337535"
                },
                "messageId": str(uuid.uuid4()),
            }
        ]
    }

    return sqs_event_message


def _sqs_bssh_event_message():
    mock_run_id = TestConstant.run_id.value
    mock_instrument_run_id = TestConstant.instrument_run_id.value
    mock_date_modified = "2020-05-09T22:17:03.1015272Z"
    mock_status = "PendingAnalysis"

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
                "body": libjson.dumps(sequence_run_message),
                "messageAttributes": ens_sqs_message_attributes,
                "attributes": {
                    "ApproximateReceiveCount": "3",
                    "SentTimestamp": "1589509337523",
                    "SenderId": "ACTGAGCTI2IGZA4XHGYYY:sender-sender",
                    "ApproximateFirstReceiveTimestamp": "1589509337535"
                },
                "eventSourceARN": "arn:aws:sqs:ap-southeast-2:843407916570:my-queue",
                "messageId": str(uuid.uuid4()),
            }
        ]
    }

    return sqs_event_message


class SQSIAPEventUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(SQSIAPEventUnitTests, self).setUp()

        self.verify_local()

        mock_labmetadata_1 = LabMetadata()
        mock_labmetadata_1.library_id = "L2000001"
        mock_labmetadata_1.sample_id = "PTC_EXPn200908LL"
        mock_labmetadata_1.override_cycles = "Y100;I8N2;I8N2;Y100"
        mock_labmetadata_1.type = LabMetadataType.WGS.value
        mock_labmetadata_1.assay = LabMetadataAssay.TSQ_NANO.value
        mock_labmetadata_1.workflow = LabMetadataWorkflow.RESEARCH.value
        mock_labmetadata_1.save()
        mock_labmetadata_2 = LabMetadata()
        mock_labmetadata_2.library_id = "L2000001_topup"
        mock_labmetadata_2.sample_id = "PTC_EXPn200908LL"
        mock_labmetadata_2.override_cycles = "Y100;I8N2;I8N2;Y100"
        mock_labmetadata_2.type = LabMetadataType.WGS.value
        mock_labmetadata_2.assay = LabMetadataAssay.TSQ_NANO.value
        mock_labmetadata_2.workflow = LabMetadataWorkflow.RESEARCH.value
        mock_labmetadata_2.save()

        when(liborca).get_sample_names_from_samplesheet(...).thenReturn(
            [
                "PTC_EXPn200908LL_L2000001",
                "PTC_EXPn200908LL_L2000001_topup"
            ]
        )

    def test_unsupported_ens_event_type(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_unsupported_ens_event_type
        """
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
                    "messageAttributes": ens_sqs_message_attributes,
                    "messageId": str(uuid.uuid4()),
                }
            ]
        }

        resp = sqs_iap_event.handler(sqs_event_message, None)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['results']), 0)  # since we skip event, assert that results is empty

    def test_sequence_run_event(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_sequence_run_event
        """
        when(libraryrun_srv).create_library_run_from_sequence(...).thenReturn(list())  # skip LibraryRun creation

        _ = sqs_iap_event.handler(_sqs_bssh_event_message(), None)

        qs = SequenceRun.objects.filter(run_id=TestConstant.run_id.value)
        sqr = qs.get()
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found SequenceRun record from db: {sqr}")

        # assert bcl convert workflow launch success and save workflow runs in portal db
        success_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, success_bcl_convert_workflow_runs.count())

    def test_sequence_run_event_emergency_stop(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_sequence_run_event_emergency_stop
        """
        when(libraryrun_srv).create_library_run_from_sequence(...).thenReturn(list())  # skip LibraryRun creation
        when(libssm).get_ssm_param(...).thenReturn(libjson.dumps([TestConstant.instrument_run_id.value]))

        _ = sqs_iap_event.handler(_sqs_bssh_event_message(), None)

        qs = SequenceRun.objects.filter(run_id=TestConstant.run_id.value)
        sqr = qs.get()
        self.assertEqual(1, qs.count())
        logger.info(f"Asserted found SequenceRun record from db: {sqr}")

        # assert bcl convert workflow is not launched
        success_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(0, success_bcl_convert_workflow_runs.count())

        self.assertRaises(SequenceRuleError)

    def test_wes_runs_event_dragen_wgs_qc(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_wes_runs_event_dragen_wgs_qc
        """
        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = _mock_bcl_convert_output()
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        sqs_iap_event.handler(
            _sqs_wes_event_message(wfv_id=mock_bcl_workflow.wfv_id, wfr_id=mock_bcl_workflow.wfr_id)
            , None
        )

        self.assertEqual(1, Workflow.objects.count())

        wgs_qc_batch_runs = [br for br in BatchRun.objects.all() if br.step == WorkflowType.DRAGEN_WGTS_QC.value]

        self.assertTrue(BatchRun.objects.count() > 1)
        self.assertTrue(wgs_qc_batch_runs[0].running)

        logger.info(f"-" * 32)
        for wfl in Workflow.objects.all():
            logger.info((wfl.wfr_id, wfl.type_name, wfl.notified))
            if wfl.type_name == WorkflowType.BCL_CONVERT.value:
                self.assertTrue(wfl.notified)

        # should call to slack webhook once for BCL Convert workflow
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_wes_runs_event_dragen_wgs_qc_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_wes_runs_event_dragen_wgs_qc_alt
        """
        mock_bcl_workflow: Workflow = WorkflowFactory()
        mock_bcl_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_bcl_workflow.notified = True  # mock also consider scenario where bcl workflow has already notified before
        mock_bcl_workflow.save()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = _mock_bcl_convert_output()
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        sqs_iap_event.handler(
            _sqs_wes_event_message(wfv_id=mock_bcl_workflow.wfv_id, wfr_id=mock_bcl_workflow.wfr_id)
            , None
        )

        # assert DRAGEN_WGS_QC workflow launch has skipped and won't save into portal db
        all_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, all_workflow_runs.count())  # should contain only one bcl convert workflow

        logger.info(f"-" * 32)
        for wfl in all_workflow_runs:
            logger.info((wfl.wfr_id, wfl.type_name, wfl.notified))

        # should not call to slack webhook
        verify(libslack.http.client.HTTPSConnection, times=0).request(...)

    def test_wes_runs_event_run_succeeded(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_wes_runs_event_run_succeeded

        Precondition:
        BCL Convert workflow is Running. Had sent notification status Running to slack sometime before...

        Scenario:
        Now, WES Run Event message arrive with RunSucceeded.
        However, checking into WES Run API endpoint says workflow is still Running status.
        Should hit WES Run History Event and be able to update run Succeeded status without issue.
        """
        mock_bcl: Workflow = WorkflowFactory()

        mock_workflow_run: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_workflow_run.status = WorkflowStatus.RUNNING.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_workflow_run)

        mock_wfl_run_history_event1: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_wfl_run_history_event1.event_id = 0
        mock_wfl_run_history_event1.timestamp = datetime.utcnow()
        mock_wfl_run_history_event1.event_type = "RunStarted"
        mock_wfl_run_history_event1.event_details = {}
        # some more task events in between ...
        mock_wfl_run_history_event2: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_wfl_run_history_event2.event_id = 46586
        mock_wfl_run_history_event2.timestamp = datetime.utcnow()
        mock_wfl_run_history_event2.event_type = WorkflowRunEventType.RUNSUCCEEDED.value
        mock_wfl_run_history_event2.event_details = {
            "output": _mock_bcl_convert_output()
        }
        mock_wfl_run_history_event_list: libwes.WorkflowRunHistoryEventList = libwes.WorkflowRunHistoryEventList()
        mock_wfl_run_history_event_list.items = [mock_wfl_run_history_event1, mock_wfl_run_history_event2]
        when(libwes.WorkflowRunsApi).list_workflow_run_history(...).thenReturn(mock_wfl_run_history_event_list)

        sqs_iap_event.handler(_sqs_wes_event_message(
            wfv_id=mock_bcl.wfv_id,
            wfr_id=mock_bcl.wfr_id,
            workflow_status=WorkflowStatus.SUCCEEDED
        ), None)

        all_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, all_workflow_runs.count())  # should contain only one Succeeded bcl convert workflow

        logger.info(f"-" * 32)
        for wfl in all_workflow_runs:
            logger.info((wfl.wfr_id, wfl.type_name, wfl.notified, wfl.end_status, wfl.end))
            logger.info(wfl.output)
            self.assertEqual(wfl.end_status, WorkflowStatus.SUCCEEDED.value)

        # should call to slack webhook 1 time to notify RunSucceeded
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_wes_runs_event_run_failed(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_wes_runs_event_run_failed

        Precondition:
        BCL Convert workflow is Running. Had sent notification status Running to slack sometime before...

        Scenario:
        Now, WES Run Event message arrive with RunFailed Internal Server Error 500.
        However, checking into WES Run API endpoint says workflow is still Running status.
        Should hit WES Run History Event and be able to update run Failed status without issue.
        """
        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_workflow_run: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_workflow_run.status = WorkflowStatus.RUNNING.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_workflow_run)

        mock_wfl_run_history_event1: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_wfl_run_history_event1.event_id = 0
        mock_wfl_run_history_event1.timestamp = datetime.utcnow()
        mock_wfl_run_history_event1.event_type = "RunStarted"
        mock_wfl_run_history_event1.event_details = {}
        # some more task events in between ...
        mock_wfl_run_history_event2: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_wfl_run_history_event2.event_id = 46586
        mock_wfl_run_history_event2.timestamp = datetime.utcnow()
        mock_wfl_run_history_event2.event_type = WorkflowRunEventType.RUNFAILED.value
        mock_wfl_run_history_event2.event_details = {
            "error": "Workflow.Failed",
            "cause": "Run Failed. Reason: task: [samplesheetSplit_launch] details: [Failed to submit TES Task. "
                     "Reason [(500)\nReason: Internal Server Error\nHTTP response headers: "
                     "HTTPHeaderDict({'Date': 'Mon, 29 Jun 2020 07:37:16 GMT', 'Content-Type': 'application/json', "
                     "'Server': 'Kestrel', 'Transfer-Encoding': 'chunked'})\nHTTP response body: "
                     "{\"code\":\"\",\"message\":\"We had an unexpected issue.  Please try your request again.  "
                     "The issue has been logged and we are looking into it.\"}\n]]"
        }
        mock_wfl_run_history_event_list: libwes.WorkflowRunHistoryEventList = libwes.WorkflowRunHistoryEventList()
        mock_wfl_run_history_event_list.items = [mock_wfl_run_history_event1, mock_wfl_run_history_event2]
        when(libwes.WorkflowRunsApi).list_workflow_run_history(...).thenReturn(mock_wfl_run_history_event_list)

        sqs_iap_event.handler(_sqs_wes_event_message(
            wfv_id=mock_bcl_workflow.wfv_id,
            wfr_id=mock_bcl_workflow.wfr_id,
            workflow_status=WorkflowStatus.FAILED
        ), None)

        # assert DRAGEN_WGS_QC workflow launch has skipped and won't save into portal db
        all_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, all_workflow_runs.count())  # should contain only one Failed bcl convert workflow

        logger.info(f"-" * 32)
        for wfl in all_workflow_runs:
            logger.info((wfl.wfr_id, wfl.type_name, wfl.notified, wfl.end_status, wfl.end))
            logger.info(wfl.output)
            self.assertEqual(wfl.end_status, WorkflowStatus.FAILED.value)

        # should call to slack webhook 1 time to notify RunFailed
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_wes_runs_event_run_failed_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_wes_runs_event_run_failed_alt

        Similar to above ^^^
        But, both WES Run and Run History API event disagree (may be much more delay) with SQS message WES EventType!
        Last resort update status using WES EventType. Most possibly RunFailed situation.
        """
        mock_bcl_workflow: Workflow = WorkflowFactory()
        mock_bcl_workflow.notified = True  # mock also consider Running status has already notified
        mock_bcl_workflow.save()

        mock_workflow_run: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_workflow_run.status = WorkflowStatus.RUNNING.value
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_workflow_run)

        mock_wfl_run_history_event1: libwes.WorkflowRunHistoryEvent = libwes.WorkflowRunHistoryEvent()
        mock_wfl_run_history_event1.event_id = 0
        mock_wfl_run_history_event1.timestamp = datetime.utcnow()
        mock_wfl_run_history_event1.event_type = "RunStarted"
        mock_wfl_run_history_event1.event_details = {}

        mock_wfl_run_history_event_list: libwes.WorkflowRunHistoryEventList = libwes.WorkflowRunHistoryEventList()
        mock_wfl_run_history_event_list.items = [mock_wfl_run_history_event1]
        when(libwes.WorkflowRunsApi).list_workflow_run_history(...).thenReturn(mock_wfl_run_history_event_list)

        sqs_iap_event.handler(_sqs_wes_event_message(
            wfv_id=mock_bcl_workflow.wfv_id,
            wfr_id=mock_bcl_workflow.wfr_id,
            workflow_status=WorkflowStatus.FAILED
        ), None)

        # assert DRAGEN_WGS_QC workflow launch has skipped and won't save into portal db
        all_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, all_workflow_runs.count())  # should contain only one Failed bcl convert workflow

        logger.info(f"-" * 32)
        for wfl in all_workflow_runs:
            logger.info((wfl.wfr_id, wfl.type_name, wfl.notified, wfl.end_status, wfl.end))
            logger.info(wfl.output)
            self.assertEqual(wfl.end_status, WorkflowStatus.FAILED.value)

        # should call to slack webhook 1 time to notify RunFailed
        verify(libslack.http.client.HTTPSConnection, times=1).request(...)

    def test_wes_runs_event_not_in_automation(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_iap_event.SQSIAPEventUnitTests.test_wes_runs_event_not_in_automation

        Scenario:
        Testing wes.runs event's workflow is not in Portal workflow runs automation database. Therefore, crash.
        That is, it might have been launched elsewhere.
        """
        wfr_id = f"wfr.{_rand(32)}"
        wfv_id = f"wfv.{_rand(32)}"

        try:
            sqs_iap_event.handler(_sqs_wes_event_message(wfv_id=wfv_id, wfr_id=wfr_id), None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

        # assert 0 workflow runs in portal db
        workflow_runs = Workflow.objects.all()
        self.assertEqual(0, workflow_runs.count())
