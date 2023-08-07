import json
from unittest import skip

from django.utils.timezone import now
from libica.app import wes

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant, DragenWtsQcWorkflowFactory, \
    WtsTumorLibraryRunFactory, WtsTumorLabMetadataFactory
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.orchestration import fastq_update_step, dragen_wts_step
from data_processors.pipeline.services import batch_srv, fastq_srv, libraryrun_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger


class DragenWtsStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wts_step.DragenWtsStepUnitTests.test_perform
        """
        self.verify_local()

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

        mock_fqlr_wts_tumor = FastqListRow()
        mock_fqlr_wts_tumor.lane = 2
        mock_fqlr_wts_tumor.rglb = TestConstant.wts_library_id_tumor.value
        mock_fqlr_wts_tumor.rgsm = TestConstant.wts_sample_id.value
        mock_fqlr_wts_tumor.rgid = f"AACTCACC.2.350702_A00130_0137_AH5KMHDSXY.{mock_fqlr_wts_tumor.rgsm}_{mock_fqlr_wts_tumor.rglb}"
        mock_fqlr_wts_tumor.read_1 = "gds://volume/path/tumor_read_1.fastq.gz"
        mock_fqlr_wts_tumor.read_2 = "gds://volume/path/tumor_read_2.fastq.gz"
        mock_fqlr_wts_tumor.save()

        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_run_with_workflow(
            library_id=mock_lbr_wts_tumor.library_id,
            lane=mock_lbr_wts_tumor.lane,
            workflow=mock_dragen_wts_qc_workflow,
        )

        results = dragen_wts_step.perform(this_workflow=mock_dragen_wts_qc_workflow)
        self.assertIsNotNone(results)

        logger.info(f"{json.dumps(results)}")
        self.assertEqual(results['subjects'][0], TestConstant.subject_id.value)


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
