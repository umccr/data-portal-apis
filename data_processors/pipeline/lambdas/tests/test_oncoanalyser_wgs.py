import json

from libumccr import aws
from libumccr.aws import libssm
from mockito import spy2, when, mock

from data_portal.models import Workflow, LabMetadata, LibraryRun
from data_portal.tests.factories import TumorNormalWorkflowFactory, TumorLabMetadataFactory, LabMetadataFactory, \
    TumorLibraryRunFactory, LibraryRunFactory
from data_processors.pipeline.domain.config import ONCOANALYSER_WGS_LAMBDA_ARN
from data_processors.pipeline.lambdas import oncoanalyser_wgs
from data_processors.pipeline.services import libraryrun_srv, workflow_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class OncoanalyserWgsUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_oncoanalyser_wgs.OncoanalyserWgsUnitTests.test_handler
        """

        mock_tumor_normal_workflow: Workflow = TumorNormalWorkflowFactory()

        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_lbr_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_normal: LibraryRun = LibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_tumor.library_id, mock_lbr_normal.library_id],
            workflow=mock_tumor_normal_workflow,
        )

        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(ONCOANALYSER_WGS_LAMBDA_ARN).thenReturn('FOO')
        mock_client = mock(aws.lambda_client())
        mock_client.invoke = mock()
        when(aws).lambda_client(...).thenReturn(mock_client)

        result = oncoanalyser_wgs.handler({
            'subject_id': mock_meta_wgs_normal.subject_id,
            'tumor_wgs_sample_id': mock_meta_wgs_tumor.sample_id,
            'tumor_wgs_library_id': mock_meta_wgs_tumor.library_id,
            'tumor_wgs_bam': "s3://umccr-research-dev/stephen/oncoanalyser_test_data/SBJ00910/wgs/bam/GRCh38_umccr/MDX210176_tumor.bam",
            'normal_wgs_sample_id': mock_meta_wgs_normal.sample_id,
            'normal_wgs_library_id': mock_meta_wgs_normal.library_id,
            'normal_wgs_bam': "s3://umccr-research-dev/stephen/oncoanalyser_test_data/SBJ00910/wgs/bam/GRCh38_umccr/MDX210175_normal.bam"
        }, None)

        logger.info("-" * 32)
        logger.info("Example oncoanalyser_wgs.handler lambda output:")
        logger.info(json.dumps(result))

        # assert oncoanalyser_wts workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(2, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get(type_name='oncoanalyser_wgs')
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(2, len(all_lib_runs))
        self.assertIn(mock_lbr_tumor, all_lib_runs)
