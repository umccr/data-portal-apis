import json

from libumccr import aws
from libumccr.aws import libssm
from mockito import spy2, when, mock

from data_portal.models import LabMetadata, LibraryRun, Workflow
from data_portal.tests.factories import WtsTumorLabMetadataFactory, WtsTumorLibraryRunFactory, TestConstant
from data_processors.pipeline.domain.config import ONCOANALYSER_WTS_LAMBDA_ARN
from data_processors.pipeline.lambdas import oncoanalyser_wts
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class StarAlignmentUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_oncoanalyser_wts.StarAlignmentUnitTests.test_handler
        """

        mock_meta_wts_tumor: LabMetadata = WtsTumorLabMetadataFactory()
        mock_lbr_wts_tumor: LibraryRun = WtsTumorLibraryRunFactory()

        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(ONCOANALYSER_WTS_LAMBDA_ARN).thenReturn('FOO')
        mock_client = mock(aws.lambda_client())
        mock_client.invoke = mock()
        when(aws).lambda_client(...).thenReturn(mock_client)

        result = oncoanalyser_wts.handler({
            "subject_id": mock_meta_wts_tumor.subject_id,
            "tumor_wts_sample_id": mock_meta_wts_tumor.sample_id,
            "tumor_wts_library_id": mock_meta_wts_tumor.library_id,
            "tumor_wts_bam": "s3://path/to/tumor.bam",
        }, None)

        logger.info("-" * 32)
        logger.info("Example star_alignment.handler lambda output:")
        logger.info(json.dumps(result))

        # assert oncoanalyser_wts workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        self.assertEqual(all_lib_runs[0].library_id, TestConstant.wts_library_id_tumor.value)
