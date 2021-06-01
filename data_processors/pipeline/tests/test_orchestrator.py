import json
import uuid
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.openapi import libwes
from mockito import when

from data_portal.models import Workflow, BatchRun, Batch, SequenceRun, LabMetadata, LabMetadataType, \
    LabMetadataPhenotype, FastqListRow
from data_portal.tests.factories import WorkflowFactory, TestConstant, SequenceRunFactory, GermlineWorkflowFactory
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import orchestrator, fastq_list_row, wes_handler
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


tn_mock_subject_id = "SBJ00001"
tn_mock_normal_read_1 = "gds://volume/path/normal_read_1.fastq.gz"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"


def build_tn_mock():
    mock_wfl_run = libwes.WorkflowRun()
    mock_wfl_run.id = TestConstant.wfr_id.value
    mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
    mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
    mock_wfl_run.output = {}
    workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
    workflow_version.id = TestConstant.wfv_id.value
    mock_wfl_run.workflow_version = workflow_version
    when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

    mock_labmetadata_normal = LabMetadata()
    mock_labmetadata_normal.subject_id = tn_mock_subject_id
    mock_labmetadata_normal.library_id = TestConstant.library_id_normal.value
    mock_labmetadata_normal.phenotype = LabMetadataPhenotype.NORMAL.value
    mock_labmetadata_normal.type = LabMetadataType.WGS.value
    mock_labmetadata_normal.save()

    mock_labmetadata_tumor = LabMetadata()
    mock_labmetadata_tumor.subject_id = tn_mock_subject_id
    mock_labmetadata_tumor.library_id = TestConstant.library_id_tumor.value
    mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
    mock_labmetadata_tumor.type = LabMetadataType.WGS.value
    mock_labmetadata_tumor.save()

    mock_flr_normal = FastqListRow()
    mock_flr_normal.lane = 1
    mock_flr_normal.rglb = TestConstant.library_id_normal.value
    mock_flr_normal.rgsm = TestConstant.sample_id.value
    mock_flr_normal.rgid = str(uuid.uuid4())
    mock_flr_normal.read_1 = tn_mock_normal_read_1
    mock_flr_normal.save()

    mock_flr_tumor = FastqListRow()
    mock_flr_tumor.lane = 2
    mock_flr_tumor.rglb = TestConstant.library_id_tumor.value
    mock_flr_tumor.rgsm = TestConstant.sample_id.value
    mock_flr_tumor.rgid = str(uuid.uuid4())
    mock_flr_tumor.read_1 = "gds://volume/path/tumor_read_1.fastq.gz"
    mock_flr_tumor.read_2 = "gds://volume/path/tumor_read_2.fastq.gz"
    mock_flr_tumor.save()


class OrchestratorUnitTests(PipelineUnitTestCase):

    def test_parse_bcl_convert_output(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_parse_bcl_convert_output
        """

        result = orchestrator.parse_bcl_convert_output(json.dumps({
            "main/fastq_list_rows": [{'rgid': "main/fastq_list_rows"}],
            "fastq_list_rows": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS"}]
        }))

        logger.info("-" * 32)
        logger.info(f"Orchestrator parse_bcl_convert_output: {json.dumps(result)}")

        self.assertEqual(result[0]['rgid'], "main/fastq_list_rows")

    def test_parse_bcl_convert_output_alt(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_parse_bcl_convert_output_alt
        """

        result = orchestrator.parse_bcl_convert_output(json.dumps({
            "fastq_list_rows2": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS"}],
            "fastq_list_rows": [{'rgid': "fastq_list_rows"}]
        }))

        logger.info("-" * 32)
        logger.info(f"Orchestrator parse_bcl_convert_output alt: {json.dumps(result)}")

        self.assertEqual(result[0]['rgid'], "fastq_list_rows")

    def test_parse_bcl_convert_output_error(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_parse_bcl_convert_output_error
        """

        try:
            orchestrator.parse_bcl_convert_output(json.dumps({
                "fastq_list_rows/main": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS"}],
                "fastq_list_rowz": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS_TOO"}]
            }))
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

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
        mock_sqr = SequenceRunFactory()

        mock_workflow = Workflow()
        mock_workflow.wfr_id = f"wfr.{_rand(32)}"
        mock_workflow.type_name = WorkflowType.BCL_CONVERT.name
        mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_workflow.sequence_run = mock_sqr
        mock_workflow.output = """
        "main/fastq_list_rows": [
            {
              "rgid": "THIS_DOES_NOT_MATTER_AS_ALREADY_MALFORMED_JSON",
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
        mock_labmetadata_tumor.type = LabMetadataType.WGS.value
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

    def test_tumor_normal(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_tumor_normal
        """
        self.verify_local()

        mock_germline_workflow: Workflow = GermlineWorkflowFactory()

        build_tn_mock()

        logger.info("-" * 32)
        when(orchestrator.demux_metadata).handler(...).thenReturn([
            {
                "sample": mock_sample_name,
                "override_cycles": "Y100;I8N2;I8N2;Y100",
                "type": "WGS",
                "assay": "TsqNano"
            }
        ])

        result = orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        logger.info("-" * 32)
        self.assertIsNotNone(result)
        logger.info(f"Orchestrator lambda call output: \n{json.dumps(result)}")

    def test_create_tn_job(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorUnitTests.test_create_tn_job
        """

        build_tn_mock()

        job_dict_list = orchestrator.create_tn_jobs(tn_mock_subject_id)
        self.assertEqual(len(job_dict_list), 1)

        job_dict = job_dict_list[0]
        self.assertEqual(job_dict['subject_id'], tn_mock_subject_id)
        self.assertEqual(job_dict['fastq_list_rows'][0]['rglb'], TestConstant.library_id_normal.value)
        self.assertEqual(job_dict['fastq_list_rows'][0]['read_1']['location'], tn_mock_normal_read_1)
        logger.info(f"Job JSON: {json.dumps(job_dict)}")


class OrchestratorIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_germline_jobs(self):
        """
        python manage.py test data_processors.pipeline.tests.test_orchestrator.OrchestratorIntegrationTests.test_prepare_germline_jobs
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
        logger.info("PREPARE GERMLINE JOBS:")

        job_list = orchestrator.prepare_germline_jobs(
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
