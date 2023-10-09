import json

from libumccr import aws
from libumccr.aws import libssm
from mockito import spy2, when, mock

from data_portal.models import LabMetadata, LibraryRun, Workflow
from data_portal.tests.factories import LabMetadataFactory, TumorLabMetadataFactory, WtsTumorLabMetadataFactory, \
    LibraryRunFactory, TumorLibraryRunFactory, WtsTumorLibraryRunFactory
from data_processors.pipeline.domain.config import ONCOANALYSER_WGTS_LAMBDA_ARN
from data_processors.pipeline.lambdas import oncoanalyser_wgts_existing_both
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class OncoanalyserWgtsExistingBothUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_oncoanalyser_wgts_existing_both.OncoanalyserWgtsExistingBothUnitTests.test_handler
        """

        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()

        mock_lbr_wgs_normal: LibraryRun = LibraryRunFactory()
        mock_lbr_wgs_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(ONCOANALYSER_WGTS_LAMBDA_ARN).thenReturn('FOO')
        mock_client = mock(aws.lambda_client())
        mock_client.invoke = mock()
        when(aws).lambda_client(...).thenReturn(mock_client)

        result = oncoanalyser_wgts_existing_both.handler({
            "subject_id": mock_meta_wgs_tumor.subject_id,
            "tumor_wgs_sample_id": mock_meta_wgs_tumor.sample_id,
            "tumor_wgs_library_id": mock_meta_wgs_tumor.library_id,
            "tumor_wgs_bam": "gds://path/to/wgs_tumor.bam",
            "tumor_wts_sample_id": mock_meta_wts_tumor.sample_id,
            "tumor_wts_library_id": mock_meta_wts_tumor.library_id,
            "tumor_wts_bam": "s3://path/to/tumor.bam",
            "normal_wgs_sample_id": mock_meta_wgs_normal.sample_id,
            "normal_wgs_library_id": mock_meta_wgs_normal.library_id,
            "normal_wgs_bam": "gds://path/to/wgs_normal.bam",
            "existing_wgs_dir": "s3://path/to/oncoanalyser/wgs/dir/",
            "existing_wts_dir": "s3://path/to/oncoanalyser/wts/dir/",
        }, None)

        logger.info("-" * 32)
        logger.info("Example oncoanalyser_wgts_existing_both.handler lambda output:")
        logger.info(json.dumps(result))
        self.assertIsNotNone(result)
        self.assertEqual(result['subject_id'], mock_meta_wts_tumor.subject_id)

        # assert oncoanalyser_wgts_existing_both workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get(type_name='oncoanalyser_wgts_existing_both')
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(3, len(all_lib_runs))
        self.assertIn(mock_lbr_wgs_normal, all_lib_runs)
        self.assertIn(mock_lbr_wgs_tumor, all_lib_runs)
        self.assertIn(mock_lbr_wts_tumor, all_lib_runs)
