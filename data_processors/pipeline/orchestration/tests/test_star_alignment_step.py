import json

from django.utils.timezone import now

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, DragenWtsQcWorkflowFactory, WtsTumorLibraryRunFactory, \
    WtsTumorLabMetadataFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import star_alignment_step
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class StarAlignmentStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_star_alignment_step.StarAlignmentStepUnitTests.test_perform
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

        sqr = mock_dragen_wts_qc_workflow.sequence_run

        mock_fqlr_wts_tumor = FastqListRow()
        mock_fqlr_wts_tumor.lane = 2
        mock_fqlr_wts_tumor.rglb = TestConstant.wts_library_id_tumor.value
        mock_fqlr_wts_tumor.rgsm = TestConstant.wts_sample_id.value
        mock_fqlr_wts_tumor.rgid = f"AACTCACC.2.350702_A00130_0137_AH5KMHDSXY.{mock_fqlr_wts_tumor.rgsm}_{mock_fqlr_wts_tumor.rglb}"
        mock_fqlr_wts_tumor.read_1 = "gds://volume/path/tumor_read_1.fastq.gz"
        mock_fqlr_wts_tumor.read_2 = "gds://volume/path/tumor_read_2.fastq.gz"
        mock_fqlr_wts_tumor.sequence_run = sqr
        mock_fqlr_wts_tumor.save()

        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_run_with_workflow(
            library_id=mock_lbr_wts_tumor.library_id,
            lane=mock_lbr_wts_tumor.lane,
            workflow=mock_dragen_wts_qc_workflow,
        )

        result = star_alignment_step.perform(this_workflow=mock_dragen_wts_qc_workflow)
        self.assertIsNotNone(result)

        logger.info(f"{json.dumps(result)}")
        self.assertEqual(result['subject_id'], TestConstant.subject_id.value)

    # TODO: Integration Test
