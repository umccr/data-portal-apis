import json

from libumccr.aws import libssm
from mockito import spy2, when

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, TumorLibraryRunFactory
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.lambdas import rnasum
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class RNAsumLambdaUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaUnitTests.test_handler
        """
        mock_tumor_library_run: LibraryRun = TumorLibraryRunFactory()

        result: dict = rnasum.handler(
            {
                "dragen_transcriptome_directory": {
                    "class": "Directory",
                    "location": "gds://path/to/WTS/output/dir"
                },
                "umccrise_directory": {
                    "class": "Directory",
                    "location": "gds://path/to/umccrise/output/dir"
                },
                "sample_name": "TUMOR_SAMPLE_ID",
                "report_directory": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
                "dataset": "BRCA",
                "subject_id": "SUBJECT_ID",
                "tumor_library_id": mock_tumor_library_run.library_id
            }, None)

        logger.info("-" * 32)
        logger.info("Example rnasum.handler lambda output:")
        logger.info(json.dumps(result))

        # assert rnasum workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        fixtures = [TestConstant.library_id_tumor.value]
        for lib_run in all_lib_runs:
            logger.info(lib_run)
            self.assertIn(lib_run.library_id, fixtures)

    def test_handler_dataset_none(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaUnitTests.test_handler_dataset_none
        """

        input_tpl = json.dumps(
            {
                "dragen_transcriptome_directory": None,
                "umccrise_directory": None,
                "sample_name": None,
                "dataset": None,
                "report_directory": None,
                "ref_data_directory": {
                    "class": "File",
                    "location": "gds://development/reference-data/rnasum/"
                }
            }
        )

        # let spy and mock the input template
        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{WorkflowType.RNASUM.value}/input").thenReturn(input_tpl)

        result: dict = rnasum.handler(
            {
                "dragen_transcriptome_directory": {
                    "class": "Directory",
                    "location": "gds://path/to/WTS/output/dir"
                },
                "umccrise_directory": {
                    "class": "Directory",
                    "location": "gds://path/to/umccrise/output/dir"
                },
                "sample_name": "TUMOR_SAMPLE_ID",
                "report_directory": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
                "dataset": None,
                "subject_id": "SUBJECT_ID",
                "tumor_library_id": "WTS_TUMOR_LIBRARY_ID"
            }, None)

        logger.info("-" * 32)
        logger.info("Example rnasum.handler lambda output:")
        logger.info(json.dumps(result))

        # assert that we detect the error with invocation
        self.assertIn('error', result.keys())

        # assert no rnasum workflow has launched and recorded workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(0, qs.count())

    def test_handler_dataset_brca(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaUnitTests.test_handler_dataset_brca

        This will use BRCA dataset in input template as default. User payload None dataset.
        """

        input_tpl = json.dumps(
            {
                "dragen_transcriptome_directory": None,
                "umccrise_directory": None,
                "sample_name": None,
                "dataset": "BRCA",  # BRCA as default dataset in input template
                "report_directory": None,
                "ref_data_directory": {
                    "class": "File",
                    "location": "gds://development/reference-data/rnasum/"
                }
            }
        )

        # let spy and mock the input template
        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{WorkflowType.RNASUM.value}/input").thenReturn(input_tpl)

        result: dict = rnasum.handler(
            {
                "dragen_transcriptome_directory": {
                    "class": "Directory",
                    "location": "gds://path/to/WTS/output/dir"
                },
                "umccrise_directory": {
                    "class": "Directory",
                    "location": "gds://path/to/umccrise/output/dir"
                },
                "sample_name": "TUMOR_SAMPLE_ID",
                "report_directory": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
                "dataset": None,  # User payload None dataset
                "subject_id": "SUBJECT_ID",
                "tumor_library_id": "WTS_TUMOR_LIBRARY_ID"
            }, None)

        logger.info("-" * 32)
        logger.info("Example rnasum.handler lambda output:")
        logger.info(json.dumps(result))

        # assert rnasum workflow has launched and recorded workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())
