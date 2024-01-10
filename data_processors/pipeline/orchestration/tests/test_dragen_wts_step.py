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
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import fastq_update_step, dragen_wts_step
from data_processors.pipeline.services import libraryrun_srv, metadata_srv
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

    def test_prepare_dragen_wts_jobs_issue_655(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wts_step.DragenWtsStepUnitTests.test_prepare_dragen_wts_jobs_issue_655

        See https://github.com/umccr/data-portal-apis/issues/655
        """
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_meta_list = [mock_meta_wts_tumor]
        job_list, subjects = dragen_wts_step.prepare_dragen_wts_jobs(mock_meta_list)

        logger.info(f"{json.dumps(job_list)}")
        logger.info(f"{json.dumps(subjects)}")

        # assert that the return `subjects` container holds some metadata
        self.assertIn(TestConstant.subject_id.value, subjects)

        # assert that the return `job_list` container is empty i.e. no FastqListRow records found for given library_id
        # so that it shall skip job submission all together
        self.assertEqual(len(job_list), 0)


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

        # from Run 175 in PROD https://umccr.slack.com/archives/C8CG6K76W/p1702091016040529
        bcl_convert_wfr_id = "wfr.c4d30704ef1d46d5a2e95860421f999d"
        # pick one WTS library_id from bcl_convert output as test fixture
        fixture_library_id = 'L2301430'
        total_jobs_to_eval = 1

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'sheets': ["2023"], 'truncate': False}, None)
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
        # - query LabMetadata table for fixture library_id
        meta_list = metadata_srv.filter_metadata_by_library_id(fixture_library_id)

        logger.info("-" * 32)
        logger.info("PREPARE DRAGEN_WTS JOBS:")

        job_list, subjects = dragen_wts_step.prepare_dragen_wts_jobs(meta_list=meta_list)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)

    @skip
    def test_prepare_dragen_wts_jobs_rerun_lib(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wts_step.DragenWtsStepIntegrationTests.test_prepare_dragen_wts_jobs_rerun_lib

        See https://github.com/umccr/data-portal-apis/issues/655
        """

        # --- pick one recent successful BCL Convert run
        # ica workflows runs list
        # ica workflows runs get wfr.<ID>

        # from Run 283 in PROD https://umccr.slack.com/archives/C8CG6K76W/p1703297926076779
        bcl_convert_wfr_id = "wfr.eff04574b4044cd081c66640fa62cc23"
        # pick one WTS library_id from bcl_convert output as test fixture
        fixture_library_id = 'L2301323'  # MLee rerun WTS library
        # it is a `rerun WTS library` so it should skip
        total_jobs_to_eval = 0

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'sheets': ["2023"], 'truncate': False}, None)
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
        # - query LabMetadata table for fixture library_id
        meta_list = metadata_srv.filter_metadata_by_library_id(fixture_library_id)

        logger.info("-" * 32)
        logger.info("PREPARE DRAGEN_WTS JOBS:")

        job_list, subjects = dragen_wts_step.prepare_dragen_wts_jobs(meta_list=meta_list)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)
