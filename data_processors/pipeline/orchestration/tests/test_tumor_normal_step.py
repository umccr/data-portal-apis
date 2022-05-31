import json
from datetime import datetime

from django.utils.timezone import make_aware, now
from libica.openapi import libwes
from libumccr.aws import libssm
from mockito import when, spy2

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType, LabMetadataWorkflow
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, DragenWgsQcWorkflowFactory, LibraryRunFactory
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.orchestration import tumor_normal_step, google_lims_update_step
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

tn_mock_subject_id = "SBJ00001"
tn_mock_normal_read_1 = "gds://volume/path/normal_read_1.fastq.gz"
normal_fastq_list_rows = []
tumor_fastq_list_rows = []


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
    mock_labmetadata_normal.workflow = LabMetadataWorkflow.CLINICAL.value
    mock_labmetadata_normal.save()

    mock_labmetadata_tumor = LabMetadata()
    mock_labmetadata_tumor.subject_id = tn_mock_subject_id
    mock_labmetadata_tumor.library_id = TestConstant.library_id_tumor.value
    mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
    mock_labmetadata_tumor.type = LabMetadataType.WGS.value
    mock_labmetadata_tumor.workflow = LabMetadataWorkflow.CLINICAL.value
    mock_labmetadata_tumor.save()

    mock_flr_normal = FastqListRow()
    mock_flr_normal.lane = 1
    mock_flr_normal.rglb = TestConstant.library_id_normal.value
    mock_flr_normal.rgsm = TestConstant.sample_id.value
    mock_flr_normal.rgid = f"AACTCACC.1.350702_A00130_0137_AH5KMHDSXY.{mock_flr_normal.rgsm}_{mock_flr_normal.rglb}"
    mock_flr_normal.read_1 = tn_mock_normal_read_1
    mock_flr_normal.save()
    normal_fastq_list_rows.append(mock_flr_normal)

    mock_flr_tumor = FastqListRow()
    mock_flr_tumor.lane = 2
    mock_flr_tumor.rglb = TestConstant.library_id_tumor.value
    mock_flr_tumor.rgsm = TestConstant.sample_id.value
    mock_flr_tumor.rgid = f"AACTCACC.2.350702_A00130_0137_AH5KMHDSXY.{mock_flr_tumor.rgsm}_{mock_flr_tumor.rglb}"
    mock_flr_tumor.read_1 = "gds://volume/path/tumor_read_1.fastq.gz"
    mock_flr_tumor.read_2 = "gds://volume/path/tumor_read_2.fastq.gz"
    mock_flr_tumor.save()
    tumor_fastq_list_rows.append(mock_flr_tumor)


class TumorNormalStepUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(TumorNormalStepUnitTests, self).setUp()

    def test_tumor_normal(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_tumor_normal_step.TumorNormalStepUnitTests.test_tumor_normal
        """
        self.verify_local()

        mock_dragen_wgs_qc_workflow: Workflow = DragenWgsQcWorkflowFactory()
        mock_dragen_wgs_qc_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_dragen_wgs_qc_workflow.end = now()
        mock_dragen_wgs_qc_workflow.output = json.dumps({
            "dragen_alignment_output_directory": {
                "location": "gds://vol/analysis_data/SBJ00001/wgs_alignment_qc/2022052276d4397b/L4200006__3_dragen",
                "class": "Directory",
            },
            "dragen_bam_out": {
                "location": "gds://vol/analysis_data/SBJ00001/wgs_alignment_qc/2022052276d4397b/L4200006__3_dragen/PRJ420003.bam",
                "basename": "PRJ420003.bam",
                "nameroot": "PRJ420003",
                "nameext": ".bam",
                "class": "File",
            }
        })
        mock_dragen_wgs_qc_workflow.save()

        mock_normal_library_run: LibraryRun = LibraryRunFactory()
        _ = libraryrun_srv.link_library_run_with_workflow(
            library_id=mock_normal_library_run.library_id,
            lane=mock_normal_library_run.lane,
            workflow=mock_dragen_wgs_qc_workflow,
        )

        build_tn_mock()

        # ignore the google lims update (that's covered elsewhere)
        when(google_lims_update_step).perform(any).thenReturn(True)

        # ignore step_skip_list
        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/step_skip_list").thenReturn(json.dumps({}))

        logger.info("-" * 32)

        result = orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        logger.info("-" * 32)
        self.assertIsNotNone(result)
        logger.info(f"Orchestrator lambda call output: {json.dumps(result)}")

        self.assertEqual(2, len(result))
        self.assertEqual(tn_mock_subject_id, result[1]['subjects'][0])

    def test_create_tn_job(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_tumor_normal_step.TumorNormalStepUnitTests.test_create_tn_job
        """

        build_tn_mock()

        job_dict = tumor_normal_step.create_tn_job(
            tumor_fastq_list_rows,
            normal_fastq_list_rows,
            tn_mock_subject_id
        )

        logger.info(f"Job JSON: {json.dumps(job_dict)}")

        self.assertEqual(job_dict['subject_id'], tn_mock_subject_id)
        self.assertEqual(job_dict['fastq_list_rows'][0]['rglb'], TestConstant.library_id_normal.value)
        self.assertEqual(job_dict['fastq_list_rows'][0]['read_1']['location'], tn_mock_normal_read_1)


class TumorNormalStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
