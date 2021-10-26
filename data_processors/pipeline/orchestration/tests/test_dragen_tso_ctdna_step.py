import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.openapi import libwes
from mockito import when

from data_portal.models.batch import Batch
from data_portal.models.batchrun import BatchRun
from data_portal.models.workflow import Workflow
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType, LabMetadataAssay
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.orchestration import fastq_update_step, dragen_tso_ctdna_step
from data_processors.pipeline.services import batch_srv, fastq_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger
from utils import wes

tn_mock_subject_id = "SBJ00001"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"


def _mock_bcl_convert_output():
    return {
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
        mock_wfl_run.output = _mock_bcl_convert_output()

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

        tso_ctdna_batch_runs = [br for br in BatchRun.objects.all() if br.step == WorkflowType.DRAGEN_TSO_CTDNA.name]
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

    def test_get_ct_tso_samplesheet_from_bcl_convert_output(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_get_ct_tso_samplesheet_from_bcl_convert_output
        """
        mock_bcl_convert_output_json_str = json.dumps(_mock_bcl_convert_output())
        ss = dragen_tso_ctdna_step.get_ct_tso_samplesheet_from_bcl_convert_output(mock_bcl_convert_output_json_str)
        logger.info(ss)
        self.assertTrue(ss.startswith('gds://'))
        self.assertTrue(ss.endswith('.csv'))

    def test_get_run_xml_files_from_bcl_convert_workflow(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_get_run_xml_files_from_bcl_convert_workflow
        """
        mock_bcl_convert_input = json.dumps({
            'bcl_input_directory': {
                "class": "Directory",
                "location": "gds://bssh-path/Runs/210701_A01052_0055_AH7KWGDSX2_r.abc123456"
            }
        })

        xml_files = dragen_tso_ctdna_step.get_run_xml_files_from_bcl_convert_workflow(mock_bcl_convert_input)
        for file in xml_files:
            logger.info(file)
            self.assertTrue(file.startswith('gds://'))
            self.assertTrue(file.endswith('.xml'))


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

        # --- pick one successful BCL Convert run in development project

        # FIXME required to switch PROD `export AWS_PROFILE=prod` as no validation run data avail in DEV yet
        #   use `ica workflows runs get wfr.xxx` to see run details
        bcl_convert_wfr_id = "wfr.41eda23e48a04cdca71b2875686c2439"
        total_jobs_to_eval = 15

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'sheet': "2020"}, None)
        labmetadata.scheduled_update_handler({'sheet': "2021"}, None)
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
            run_step=WorkflowType.DRAGEN_TSO_CTDNA.value.upper(),
            batch_srv=batch_srv,
            fastq_srv=fastq_srv,
            logger=logger
        )

        print('-'*32)
        logger.info("PREPARE DRAGEN_TSO_CTDNA JOBS:")

        job_list = dragen_tso_ctdna_step.prepare_dragen_tso_ctdna_jobs(batcher)

        print('-'*32)
        logger.info("JOB LIST JSON:")
        print()
        print(json.dumps(job_list))
        print()
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)
