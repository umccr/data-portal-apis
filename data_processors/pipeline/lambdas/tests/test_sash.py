import json

from libumccr import aws
from libumccr.aws import libssm
from mockito import spy2, when, mock

from data_portal.models import Workflow, LibraryRun
from data_portal.tests.factories import TumorLibraryRunFactory, LibraryRunFactory
from data_processors.pipeline.domain.config import SASH_LAMBDA_ARN
from data_processors.pipeline.lambdas import sash
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class SashUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sash.SashUnitTests.test_handler
        """
        mock_lbr_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_normal: LibraryRun = LibraryRunFactory()

        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(SASH_LAMBDA_ARN).thenReturn('FOO')
        mock_client = mock(aws.lambda_client())
        mock_client.invoke = mock()
        when(aws).lambda_client(...).thenReturn(mock_client)

        result = sash.handler({
            "subject_id": "SBJ00001",
            "tumor_sample_id": "PRJ230001",
            "tumor_library_id": mock_lbr_tumor.library_id,
            "normal_sample_id": "PRJ230002",
            "normal_library_id": mock_lbr_normal.library_id,
            "dragen_somatic_dir": "gds://production/analysis_data/SBJ00001/wgs_tumor_normal/20230515zyxwvuts/L2300001_L2300002/",
            "dragen_germline_dir": "gds://production/analysis_data/SBJ00001/wgs_tumor_normal/20230515zyxwvuts/L2300002_dragen_germline/",
            "oncoanalyser_dir": "s3://org.umccr.data.oncoanalyser/analysis_data/SBJ00001/oncoanalyser/20230518poiuytre/wgs/L2300001__L2300002/SBJ00001_PRJ230001/"
        }, None)

        logger.info("-" * 32)
        logger.info("Example sash.handler lambda output:")
        logger.info(json.dumps(result))

        # assert sash workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get(type_name='sash')
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(2, len(all_lib_runs))
        self.assertIn(mock_lbr_tumor, all_lib_runs)
        self.assertIn(mock_lbr_normal, all_lib_runs)
        self.assertEqual(result['normal_library_id'], mock_lbr_normal.library_id)
