import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware, now
from libica.app import wes
from libica.openapi import libwes
from mockito import when

from data_portal.models.batch import Batch
from data_portal.models.batchrun import BatchRun
from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType, LabMetadataWorkflow
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant, DragenWtsQcWorkflowFactory, \
    WtsTumorLibraryRunFactory, LibraryRunFactory
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.orchestration import fastq_update_step, dragen_wts_step
from data_processors.pipeline.services import batch_srv, fastq_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

wts_mock_subject_id = "SBJ00001"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"
tumor_fastq_list_rows = []


def build_wts_mock():
    mock_wfl_run = libwes.WorkflowRun()
    mock_wfl_run.id = TestConstant.wfr_id.value
    mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
    mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
    mock_wfl_run.output = {}
    workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
    workflow_version.id = TestConstant.wfv_id.value
    mock_wfl_run.workflow_version = workflow_version
    when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

    mock_labmetadata_tumor = LabMetadata()
    mock_labmetadata_tumor.subject_id = wts_mock_subject_id
    mock_labmetadata_tumor.library_id = mock_library_id
    mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
    mock_labmetadata_tumor.type = LabMetadataType.WTS.value
    mock_labmetadata_tumor.workflow = LabMetadataWorkflow.CLINICAL.value
    mock_labmetadata_tumor.save()

    mock_flr_tumor = FastqListRow()
    mock_flr_tumor.lane = 2
    mock_flr_tumor.rglb = TestConstant.library_id_tumor.value
    mock_flr_tumor.rgsm = TestConstant.sample_id.value
    mock_flr_tumor.rgid = f"AACTCACC.2.350702_A00130_0137_AH5KMHDSXY.{mock_flr_tumor.rgsm}_{mock_flr_tumor.rglb}"
    mock_flr_tumor.read_1 = "gds://volume/path/tumor_read_1.fastq.gz"
    mock_flr_tumor.read_2 = "gds://volume/path/tumor_read_2.fastq.gz"
    mock_flr_tumor.save()
    tumor_fastq_list_rows.append(mock_flr_tumor)


class DragenWtsStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wts_step.DragenWtsStepUnitTests.test_perform
        """
        self.verify_local()

        build_wts_mock()

        mock_dragen_wts_qc_workflow: Workflow = DragenWtsQcWorkflowFactory()
        mock_dragen_wts_qc_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_dragen_wts_qc_workflow.end = now()
        mock_dragen_wts_qc_workflow.output = json.dumps({
            "dragen_alignment_output_directory": {
                "location": "gds://vol/analysis_data/SBJ00001/wts_alignment_qc/2022052276d4397b/LPRJ200438__3_dragen",
                "class": "Directory",
            },
            "dragen_bam_out": {
                "location": "gds://vol/analysis_data/SBJ00001/wts_alignment_qc/2022052276d4397b/LPRJ200438__3_dragen/PRJ200438.bam",
                "basename": "PRJ200438.bam",
                "nameroot": "PRJ200438",
                "nameext": ".bam",
                "class": "File",
            }
        })
        mock_dragen_wts_qc_workflow.save()
        mock_lbr_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        results = dragen_wts_step.perform(this_workflow=mock_dragen_wts_qc_workflow)
        self.assertIsNotNone(results)

        logger.info(f"{json.dumps(results)}")
        self.assertEqual(results['submitting_subjects'][0], wts_mock_subject_id)


class DragenWtsStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_dragen_wts_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wts_step.DragenWtsStepIntegrationTests.test_prepare_dragen_wts_jobs
        """

        # --- pick one recent successful BCL Convert run
        # ica workflows runs list
        # ica workflows runs get wfr.<ID>

        bcl_convert_wfr_id = "wfr.8885338040b542f290c9bf6b7e0c4a36"  # from Run 171 in PROD
        total_jobs_to_eval = 8

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'sheets': ["2020", "2021"], 'truncate': False}, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # second --
        # - we need to have BCL Convert workflow in db
        # - WorkflowFactory also create related fixture sub factory SequenceRunFactory and linked them
        mock_bcl_convert: Workflow = WorkflowFactory()

        # third --
        # - grab workflow run from WES endpoint
        # - sync input and output attributes to our mock BCL Convert workflow in db
        bcl_convert_run = wes.get_run(bcl_convert_wfr_id, to_dict=True)
        mock_bcl_convert.input = json.dumps(bcl_convert_run['input'])
        mock_bcl_convert.output = json.dumps(bcl_convert_run['output'])
        mock_bcl_convert.save()

        # fourth --
        # - replay FastqListRow update step after BCL Convert workflow succeeded
        fastq_update_step.perform(mock_bcl_convert)

        # fifth --
        # - we also need Batch and BatchRun since DRAGEN_WTS workflows (jobs) are running in batch manner
        # - we will use Batcher to create them
        batcher = Batcher(
            workflow=mock_bcl_convert,
            run_step=WorkflowType.DRAGEN_WTS.value,
            batch_srv=batch_srv,
            fastq_srv=fastq_srv,
            logger=logger
        )

        logger.info("-" * 32)
        logger.info("PREPARE DRAGEN_WTS JOBS:")

        job_list = dragen_wts_step.prepare_dragen_wts_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)
