import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.openapi import libwes
from mockito import when

from data_portal.models import Batch, BatchRun, SequenceRun, Workflow, LabMetadata, LabMetadataPhenotype, \
    LabMetadataType, LabMetadataAssay
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.lambdas import wes_handler, fastq_list_row, orchestrator
from data_processors.pipeline.orchestration import dragen_tso_ctdna_step
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

tn_mock_subject_id = "SBJ00001"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"


class DragenTsoCtDnaStepUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(DragenTsoCtDnaStepUnitTests, self).setUp()

    def test_dragen_tso_ctdna(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_dragen_tso_ctdna
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()
        mock_bcl_workflow.input = json.dumps({
            'bcl_input_directory': {
                "class": "Directory",
                "location": "gds://bssh-path/Runs/210701_A01052_0055_AH7KWGDSX2_r.abc123456"
            }
        })
        mock_bcl_workflow.save()
        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            "fastq_list_rows": [
                {
                    "rgid": "CATGCGAT.4",
                    "rglb": "UnknownLibrary",
                    "rgsm": mock_sample_name,
                    "lane": 4,
                    "read_1": {
                        "class": "File",
                        "basename": f"{mock_sample_name}_S1_L004_R1_001.fastq.gz",
                        "location": f"gds://fastqvol/bcl-convert-test/outputs/10X/{mock_sample_name}_S1_L004_R1_001.fastq.gz",
                        "nameroot": f"{mock_sample_name}_S1_L004_R1_001.fastq",
                        "nameext": ".gz",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 16698849950
                    },
                    "read_2": {
                        "class": "File",
                        "basename": f"{mock_sample_name}_S1_L004_R2_001.fastq.gz",
                        "location": f"gds://fastqvol/bcl-convert-test/outputs/10X/{mock_sample_name}_S1_L004_R2_001.fastq.gz",
                        "nameroot": f"{mock_sample_name}_S1_L004_R2_001.fastq",
                        "nameext": ".gz",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 38716143739
                    }
                }
            ],
            "split_sheets": [
                              {
                                "location": "gds://umccr-fastq-data-prod/210701_A01052_0055_AH7KWGDSX2/SampleSheet.ctDNA_ctTSO.csv",
                                "basename": "SampleSheet.ctDNA_ctTSO.csv",
                                "nameroot": "SampleSheet.ctDNA_ctTSO",
                                "nameext": ".csv",
                                "class": "File",
                                "size": 1804,
                                "http://commonwl.org/cwltool#generation": 0
                              }
            ]
        }

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        #LPRJ200438
        mock_labmetadata_tumor = LabMetadata()
        mock_labmetadata_tumor.subject_id = tn_mock_subject_id
        mock_labmetadata_tumor.library_id = mock_library_id
        mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
        mock_labmetadata_tumor.type = LabMetadataType.CT_DNA.value
        mock_labmetadata_tumor.assay = LabMetadataAssay.CT_TSO.value
        mock_labmetadata_tumor.save()

        result = orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        logger.info("-" * 32)
        self.assertIsNotNone(result)
        logger.info(f"Orchestrator lambda call output: \n{json.dumps(result)}")

        for b in Batch.objects.all():
            logger.info(f"BATCH: {b}")
        for br in BatchRun.objects.all():
            logger.info(f"BATCH_RUN: {br}")
        tso_ctdna_batch_runs = [batchrun
                                for batchrun in BatchRun.objects.all()
                                if batchrun.step == "DRAGEN_TSO_CTDNA"]
        self.assertTrue(tso_ctdna_batch_runs[0].running)

    def test_dragen_tso_ctdna_none(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_dragen_tso_ctdna_none

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


class DragenTsoCtDnaStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_dragen_tso_ctdna_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepIntegrationTests.test_prepare_dragen_tso_ctdna_jobs
        """

        # --- Setup these values

        bssh_run_id = "r.Uvlx2DEIME-KH0BRyF9XBg"
        bssh_run_name = "200612_A01052_0017_BH5LYWDSXY"
        bssh_run_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg"
        bssh_run_volume = "umccr-raw-sequence-data-dev"
        bssh_samplesheet_name = "SampleSheet.csv"
        bcl_convert_wfr_id = "wfr.81cf25d7226a4874be43e4b15c1f5687"

        # --- Get pipeline state

        bcl_convert_run = wes_handler.get_workflow_run({'wfr_id': bcl_convert_wfr_id}, None)

        print('-' * 32)

        fastq_list = fastq_list_row.handler({
            'fastq_list_rows': bcl_convert_run['output']['fastq_list_rows'],
            'seq_name': bssh_run_name
        }, None)

        # --- Make mock pipeline state in test db

        mock_batch = Batch(name="Test", created_by="Test", context_data=json.dumps(fastq_list))
        mock_batch.save()

        mock_batch_run = BatchRun(batch=mock_batch, step="Test")
        mock_batch_run.save()

        mock_sqr_run = SequenceRun(
            run_id=bssh_run_id,
            date_modified=make_aware(datetime.utcnow()),
            status="PendingAnalysis",
            instrument_run_id=bssh_run_name,
            gds_folder_path=bssh_run_path,
            gds_volume_name=bssh_run_volume,
            reagent_barcode="NV9999999-RGSBS",
            v1pre3_id="666666",
            acl=["wid:acgtacgt-9999-38ed-99fa-94fe79523959"],
            flowcell_barcode="BARCODEEE",
            sample_sheet_name=bssh_samplesheet_name,
            api_url=f"https://ilmn/v2/runs/r.ACGTlKjDgEy099ioQOeOWg",
            name=bssh_run_name,
            msg_attr_action="statuschanged",
            msg_attr_action_type="bssh.runs",
            msg_attr_action_date="2020-05-09T22:17:10.815Z",
            msg_attr_produced_by="BaseSpaceSequenceHub"
        )
        mock_sqr_run.save()

        print('-'*32)
        logger.info("PREPARE DRAGEN_TSO_CTDNA JOBS:")

        job_list = dragen_tso_ctdna_step.prepare_dragen_tso_ctdna_jobs(
            this_batch=mock_batch,
            this_batch_run=mock_batch_run,
            this_sqr=mock_sqr_run,
        )

        print('-'*32)
        logger.info("JOB LIST JSON:")
        print()
        print(json.dumps(job_list))
        print()
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 8)
