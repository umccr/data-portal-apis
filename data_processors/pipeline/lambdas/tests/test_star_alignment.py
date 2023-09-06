import json

import boto3
from libumccr import aws
from libumccr.aws import libssm
from mockito import when, spy2, mock

from data_portal.models.workflow import Workflow
from data_portal.models.libraryrun import LibraryRun
from data_portal.tests.factories import TestConstant, LibraryRunFactory, WtsTumorLabMetadataFactory, \
    WtsTumorLibraryRunFactory
from data_processors.pipeline.domain.config import STAR_ALIGNMENT_LAMBDA_ARN, ICA_WORKFLOW_PREFIX
from data_processors.pipeline.lambdas import star_alignment
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class StarAlignmentUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_star_alignment.StarAlignmentUnitTests.test_handler
        """
        """
        Expected payload:
            {
                "subject_id": subject_id,
                "sample_id": rgsm,
                "library_id": library_id,
                "fastq_fwd": fastq_read_1,
                "fastq_rev": fastq_read_2,
            }
        """

        payload = {
            "subject_id": "subject_id",
            "sample_id": "rgsm",
            "library_id": TestConstant.wts_library_id_tumor.value,
            "fastq_fwd": "fastq_read_1",
            "fastq_rev": "fastq_read_2",
        }

        _ = WtsTumorLabMetadataFactory()
        _ = WtsTumorLibraryRunFactory()

        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(STAR_ALIGNMENT_LAMBDA_ARN).thenReturn('FOO')
        mock_client = mock(aws.lambda_client())
        mock_client.invoke = mock()
        when(aws).lambda_client(...).thenReturn(mock_client)

        result: dict = star_alignment.handler(payload, None)

        logger.info("-" * 32)
        logger.info("Example star_alignment.handler lambda output:")
        logger.info(json.dumps(result))

        # assert star_alignment workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        self.assertEqual(all_lib_runs[0].library_id, TestConstant.wts_library_id_tumor.value)
