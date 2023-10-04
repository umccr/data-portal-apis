import json

from django.utils.timezone import now

from data_portal.models import S3Object
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, StarAlignmentWorkflowFactory, \
    WtsTumorLabMetadataFactory, WtsTumorLibraryRunFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import oncoanalyser_wts_step
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.services.tests import test_s3object_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


def generate_mock_star_alignment_bam():
    mock_portal_run_id = TestConstant.portal_run_id.value

    mock_bam = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/PRJ421001.md.bam",
        size=1000,
        last_modified_date=now(),
        e_tag="abcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/PRJ421001.md.bam.bai",
        size=1000,
        last_modified_date=now(),
        e_tag="abcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/",
        size=1100,
        last_modified_date=now(),
        e_tag="bbcdefghi123456"
    )
    _ = S3Object.objects.create(
        bucket="bucket1",
        key=f"analysis_data/SBJ00001/star-align-nf/{mock_portal_run_id}/L4200001/PRJ421001/PRJ421001.vcf",
        size=1001,
        last_modified_date=now(),
        e_tag="cccdefghi123456"
    )

    return mock_bam


class OncoanalyserWtsStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wts_step.OncoanalyserWtsStepUnitTests.test_perform
        """
        self.verify_local()

        mock_star_alignment_workflow: Workflow = StarAlignmentWorkflowFactory()
        mock_star_alignment_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_star_alignment_workflow.end = now()
        mock_star_alignment_workflow.input = json.dumps({
            "portal_run_id": mock_star_alignment_workflow.portal_run_id,
            "subject_id": TestConstant.subject_id.value,
            "sample_id": TestConstant.wts_sample_id.value,
            "library_id": TestConstant.wts_library_id_tumor.value,
            "fastq_fwd": "fastq_fwd.fastq.gz",
            "fastq_rev": "fastq_rev.fastq.gz"
        })
        mock_star_alignment_workflow.save()

        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_wts_tumor.library_id],
            workflow=mock_star_alignment_workflow,
        )

        _ = generate_mock_star_alignment_bam()

        result = oncoanalyser_wts_step.perform(this_workflow=mock_star_alignment_workflow)
        self.assertIsNotNone(result)

        logger.info(f"{json.dumps(result)}")
        self.assertEqual(result['subject_id'], TestConstant.subject_id.value)
        self.assertEqual(result['tumor_wts_sample_id'], mock_meta_wts_tumor.sample_id)
        self.assertEqual(result['tumor_wts_library_id'], mock_lbr_wts_tumor.library_id)

    def test_get_star_alignment_output_bam(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wts_step.OncoanalyserWtsStepUnitTests.test_get_star_alignment_output_bam
        """

        _ = generate_mock_star_alignment_bam()

        bam_loc = oncoanalyser_wts_step.get_star_alignment_output_bam(portal_run_id=TestConstant.portal_run_id.value)
        logger.info(bam_loc)
        self.assertIsNotNone(bam_loc)

    def test_get_star_alignment_no_output_bam(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wts_step.OncoanalyserWtsStepUnitTests.test_get_star_alignment_no_output_bam
        """
        try:
            _ = oncoanalyser_wts_step.get_star_alignment_output_bam(portal_run_id=TestConstant.portal_run_id.value)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_get_star_alignment_multiple_output_bam(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wts_step.OncoanalyserWtsStepUnitTests.test_get_star_alignment_multiple_output_bam
        """

        _ = test_s3object_srv.generate_mock_data()

        try:
            _ = oncoanalyser_wts_step.get_star_alignment_output_bam(portal_run_id=TestConstant.portal_run_id.value)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)
