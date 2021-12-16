import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.app import wes
from libica.openapi import libwes
from mockito import when

from data_portal.models.batch import Batch
from data_portal.models.batchrun import BatchRun
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.orchestration import dragen_wgs_qc_step, fastq_update_step
from data_processors.pipeline.services import batch_srv, fastq_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

tn_mock_subject_id = "SBJ00001"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"


class DragenWgsQcStepUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(DragenWgsQcStepUnitTests, self).setUp()

    def test_dragen_wgs_qc(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepUnitTests.test_dragen_wgs_qc
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

        wgs_qc_batch_runs = [br for br in BatchRun.objects.all() if br.step == WorkflowType.DRAGEN_WGS_QC.value]

        # FIXME holiday patch
        # self.assertTrue(wgs_qc_batch_runs[0].running)
        self.assertEqual(0, len(wgs_qc_batch_runs))  # assert that qc workflow don't trigger!

    def test_dragen_wgs_qc_none(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepUnitTests.test_dragen_wgs_qc_none

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


class DragenWgsQcStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_dragen_wgs_qc_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepIntegrationTests.test_prepare_dragen_wgs_qc_jobs
        """

        # --- pick one successful BCL Convert run in development project
        # ica workflows runs list
        # ica workflows runs get wfr.<ID>

        bcl_convert_wfr_id = "wfr.18210c790f30452992c5fd723521f014"
        total_jobs_to_eval = 12

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2020", "2021"],
            'truncate': False
        }, None)
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
        # - we also need Batch and BatchRun since DRAGEN_WGS_QC workflows (jobs) are running in batch manner
        # - we will use Batcher to create them, just like in dragen_wgs_qc_step.perform()
        batcher = Batcher(
            workflow=mock_bcl_convert,
            run_step=WorkflowType.DRAGEN_WGS_QC.value,
            batch_srv=batch_srv,
            fastq_srv=fastq_srv,
            logger=logger
        )

        logger.info("-" * 32)
        logger.info("PREPARE DRAGEN_WGS_QC JOBS:")

        job_list = dragen_wgs_qc_step.prepare_dragen_wgs_qc_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)
