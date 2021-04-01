import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libiap.openapi import libwes, libgds
from mockito import when

from data_portal.models import Workflow, BatchRun, Batch, SequenceRun
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class OrchestratorUnitTests(PipelineUnitTestCase):

    def test_workflow_output_not_json(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_workflow_output_not_json

        Should raise:
            [ERROR] JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
            ...
            ...
            json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes: line 3 column 13 (char 23)

        Storing models.Workflow.output into database should always be in JSON format.
        """
        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.name
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.output = """
        "main/fastq_list_rows": [
            {
              "rgid": "CATGCGAT.4",
              "rglb": "UnknownLibrary",
              "rgsm": "PRJ200438_LPRJ200438",
              "lane": 4,
              "read_1": {
                "class": "File",
                "basename": "PRJ200438_LPRJ200438_S1_L004_R1_001.fastq.gz",
                "location": "gds://wfr.71527fbd4798426798811ffd1cd8f010/bcl-convert-test/outputs/10X/PRJ200438_LPRJ200438_S1_L004_R1_001.fastq.gz",
                "nameroot": "PRJ200438_LPRJ200438_S1_L004_R1_001.fastq",
                "nameext": ".gz",
                "http://commonwl.org/cwltool#generation": 0,
                "size": 16698849950
              },
              "read_2": {
                "class": "File",
                "basename": "PRJ200438_LPRJ200438_S1_L004_R2_001.fastq.gz",
                "location": "gds://wfr.71527fbd4798426798811ffd1cd8f010/bcl-convert-test/outputs/10X/PRJ200438_LPRJ200438_S1_L004_R2_001.fastq.gz",
                "nameroot": "PRJ200438_LPRJ200438_S1_L004_R2_001.fastq",
                "nameext": ".gz",
                "http://commonwl.org/cwltool#generation": 0,
                "size": 38716143739
              }
            }
        ]
        """
        try:
            orchestrator.next_step(mock_workflow, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_germline(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_germline
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            "main/fastq_list_rows": [
                {
                    "rgid": "CATGCGAT.4",
                    "rglb": "UnknownLibrary",
                    "rgsm": "PRJ200438_LPRJ200438",
                    "lane": 4,
                    "read_1": {
                        "class": "File",
                        "basename": "PRJ200438_LPRJ200438_S1_L004_R1_001.fastq.gz",
                        "location": "gds://wfr.71527fbd4798426798811ffd1cd8f010/bcl-convert-test/outputs/10X/PRJ200438_LPRJ200438_S1_L004_R1_001.fastq.gz",
                        "nameroot": "PRJ200438_LPRJ200438_S1_L004_R1_001.fastq",
                        "nameext": ".gz",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 16698849950
                    },
                    "read_2": {
                        "class": "File",
                        "basename": "PRJ200438_LPRJ200438_S1_L004_R2_001.fastq.gz",
                        "location": "gds://wfr.71527fbd4798426798811ffd1cd8f010/bcl-convert-test/outputs/10X/PRJ200438_LPRJ200438_S1_L004_R2_001.fastq.gz",
                        "nameroot": "PRJ200438_LPRJ200438_S1_L004_R2_001.fastq",
                        "nameext": ".gz",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 38716143739
                    }
                }
            ]
        }

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        when(orchestrator.demux_metadata).handler(...).thenReturn([
            {
                "sample": "PRJ200438_LPRJ200438",
                "override_cycles": "Y100;I8N2;I8N2;Y100",
                "type": "WGS",
                "assay": "TsqNano"
            }
        ])

        result = orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        self.assertIsNotNone(result)
        logger.info(f"Orchestrator lambda call output: \n{json.dumps(result)}")

        for b in Batch.objects.all():
            logger.info(f"BATCH: {b}")
        for br in BatchRun.objects.all():
            logger.info(f"BATCH_RUN: {br}")
        self.assertTrue(BatchRun.objects.all()[0].running)

    def test_germline_none(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_germline_none

        Similar to ^^ test case but BCL Convert output is None
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = None  # mock output is NONE while status is SUCCEEDED

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        try:
            orchestrator.handler({
                'wfr_id': TestConstant.wfr_id.value,
                'wfv_id': TestConstant.wfv_id.value,
            }, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_bcl_unknown_type(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_bcl_unknown_type

        Similar to ^^ test case but BCL Convert output is not list nor dict
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            'main/fastqs': "say, for example, cwl workflow output is some malformed string, oh well :("
        }

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        try:
            orchestrator.handler({
                'wfr_id': TestConstant.wfr_id.value,
                'wfv_id': TestConstant.wfv_id.value,
            }, None)
        except Exception as e:
            for b in Batch.objects.all():
                logger.info(f"BATCH: {b}")
            for br in BatchRun.objects.all():
                logger.info(f"BATCH_RUN: {br}")
            self.assertFalse(BatchRun.objects.all()[0].running)

            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")

        self.assertRaises(json.JSONDecodeError)


class OrchestratorIntegrationTests(PipelineIntegrationTestCase):

    @skip
    def test_prepare_germline_jobs(self):
        """
        1. uncomment @skip
        2. setup target_xx variables noted below
        3. hit the test like so:
            python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorIntegrationTests.test_prepare_germline_jobs
        """

        # NOTE: need to set the target bssh run gds output
        target_gds_folder_path = "/Runs/111111_A22222_0011_AGCTG2AGCC_r.ACGTlKjDgEy099ioQOeOWg"
        target_gds_volume_name = "bssh.agctbfda498038ed99eeeeee79999999"
        target_sample_sheet_name = "SampleSheet.csv"

        # NOTE: typically dict within json.dumps is the output of fastq lambda. See wiki for how to invoke fastq lambda
        # https://github.com/umccr/wiki/blob/master/computing/cloud/illumina/automation.md
        target_batch_context_data = json.dumps(
            [
                {"lane": 4,
                 "read_1": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz",
                 "read_2": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq.gz",
                 "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                 "rgsm": "PTC_TSOctDNA200901VD_L2000753",
                 "rglb": "UnknownLibrary"
                 },
                {
                    "lane": 3,
                    "read_1": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq.gz",
                    "read_2": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq.gz",
                    "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                    "rgsm": "PTC_TSOctDNA200901VD_L2000753_topup",
                    "rglb": "UnknownLibrary"
                }
            ]
        )

        mock_batch = Batch(name="Test", created_by="Test", context_data=target_batch_context_data)
        mock_batch.save()

        mock_batch_run = BatchRun(batch=mock_batch)
        mock_batch_run.save()

        mock_sqr_run = SequenceRun(
            run_id="r.ACGTlKjDgEy099ioQOeOWg",
            date_modified=make_aware(datetime.utcnow()),
            status="PendingAnalysis",
            instrument_run_id=TestConstant.sqr_name.value,
            gds_folder_path=target_gds_folder_path,
            gds_volume_name=target_gds_volume_name,
            reagent_barcode="NV9999999-RGSBS",
            v1pre3_id="666666",
            acl=["wid:acgtacgt-9999-38ed-99fa-94fe79523959"],
            flowcell_barcode="BARCODEEE",
            sample_sheet_name=target_sample_sheet_name,
            api_url=f"https://ilmn/v2/runs/r.ACGTlKjDgEy099ioQOeOWg",
            name=TestConstant.sqr_name.value,
            msg_attr_action="statuschanged",
            msg_attr_action_type="bssh.runs",
            msg_attr_action_date="2020-05-09T22:17:10.815Z",
            msg_attr_produced_by="BaseSpaceSequenceHub"
        )
        mock_sqr_run.save()

        job_list = orchestrator.prepare_germline_jobs(
            this_batch=mock_batch,
            this_batch_run=mock_batch_run,
            this_sqr=mock_sqr_run,
        )

        print(json.dumps(job_list))
        self.assertIsNotNone(job_list)
