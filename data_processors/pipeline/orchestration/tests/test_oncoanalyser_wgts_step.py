import json

from django.utils.timezone import now

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, StarAlignmentWorkflowFactory, \
    TumorLabMetadataFactory, TumorLibraryRunFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import oncoanalyser_wgts_step
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class OncoanalyserWgtsStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgts_step.OncoanalyserWgtsStepUnitTests.test_perform
        """
        self.verify_local()
        logger.info("STARTING TEST")

        mock_star_alignment_workflow: Workflow = StarAlignmentWorkflowFactory()
        mock_star_alignment_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_star_alignment_workflow.end = now()
        mock_star_alignment_workflow.input = json.dumps({
            "portal_run_id": mock_star_alignment_workflow.portal_run_id,
            "subject_id": TestConstant.subject_id.value,
            "sample_id": TestConstant.sample_id.value,
            "library_id": TestConstant.library_id_tumor.value,
            "fastq_fwd": "fastq_fwd.fastq.gz",
            "fastq_rev": "fastq_rev.fastq.gz"
        })
        mock_star_alignment_workflow.save()

        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_lbr_tumor: LibraryRun = TumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_tumor.library_id],
            workflow=mock_star_alignment_workflow,
        )

        result = oncoanalyser_wts_step.perform(this_workflow=mock_star_alignment_workflow)
        self.assertIsNotNone(result)

        logger.info(f"{json.dumps(result)}")
        self.assertEqual(result['subject_id'], TestConstant.subject_id.value)
        self.assertEqual(result['tumor_wts_sample_id'], mock_meta_wgs_tumor.sample_id)
        self.assertEqual(result['tumor_wts_library_id'], mock_lbr_tumor.library_id)

        bam_file = oncoanalyser_wts_step.construct_bam_location(
            mock_star_alignment_workflow.portal_run_id,
            str(TestConstant.subject_id.value),  # cast to str to avoid typing warning
            mock_meta_wgs_tumor.sample_id,
            mock_lbr_tumor.library_id
        )
        self.assertEqual(result['tumor_wts_bam'], bam_file)

    # TODO: Integration Test