import json

from libumccr.aws import libssm
from mockito import spy2, when

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, TumorLibraryRunFactory, RNAsumWorkflowFactory
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import rnasum
from data_processors.pipeline.orchestration.tests import test_rnasum_step
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
                "dragen_wts_dir": {
                    "class": "Directory",
                    "location": "gds://path/to/WTS/output/dir"
                },
                "umccrise": {
                    "class": "Directory",
                    "location": "gds://path/to/umccrise/output/dir"
                },
                "arriba_dir": {
                    "class": "Directory",
                    "location": "gds://path/to/arriba/output/dir"
                },
                "sample_name": "TUMOR_SAMPLE_ID",
                "report_dir": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
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
                "dragen_wts_dir": None,
                "umccrise": None,
                "arriba_dir": None,
                "sample_name": None,
                "dataset": None,
                "report_dir": None
            }
        )

        # let spy and mock the input template
        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{WorkflowType.RNASUM.value}/input").thenReturn(input_tpl)

        result: dict = rnasum.handler(
            {
                "dragen_wts_dir": {
                    "class": "Directory",
                    "location": "gds://path/to/WTS/output/dir"
                },
                "umccrise": {
                    "class": "Directory",
                    "location": "gds://path/to/umccrise/output/dir"
                },
                "arriba_dir": {
                    "class": "Directory",
                    "location": "gds://path/to/arriba/output/dir"
                },
                "sample_name": "TUMOR_SAMPLE_ID",
                "report_dir": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
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
                "dragen_wts_dir": None,
                "umccrise": None,
                "arriba_dir": None,
                "sample_name": None,
                "dataset": "BRCA",  # BRCA as default dataset in input template
                "report_dir": None
            }
        )

        # let spy and mock the input template
        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{WorkflowType.RNASUM.value}/input").thenReturn(input_tpl)

        result: dict = rnasum.handler(
            {
                "dragen_wts_dir": {
                    "class": "Directory",
                    "location": "gds://path/to/WTS/output/dir"
                },
                "umccrise": {
                    "class": "Directory",
                    "location": "gds://path/to/umccrise/output/dir"
                },
                "arriba_dir": {
                    "class": "Directory",
                    "location": "gds://path/to/arriba/output/dir"
                },
                "sample_name": "TUMOR_SAMPLE_ID",
                "report_dir": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
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


class RNAsumLambdaExtendedUnitTests(PipelineUnitTestCase):
    """
    This is extended test cases.
    See note in
        data_processors.pipeline.lambdas.rnasum.by_umccrise_handler
        data_processors.pipeline.lambdas.rnasum.by_subject_handler
    """

    def test_rnasum_by_umccrise_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaExtendedUnitTests.test_rnasum_by_umccrise_handler
        """
        self.verify_local()

        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()

        results = rnasum.by_umccrise_handler(event={
            "wfr_id": mock_umccrise_workflow.wfr_id,
            "dataset": "BLCA"
        }, context=None)

        submitted_dataset = results['job_list'][0]['dataset']
        self.assertEqual(submitted_dataset, "BLCA")

        logger.info("-" * 32)
        logger.info("Example rnasum.by_umccrise_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_by_umccrise_handler_wfr_not_exist(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaExtendedUnitTests.test_rnasum_by_umccrise_handler_wfr_not_exist
        """

        results = rnasum.by_umccrise_handler(event={
            "wfr_id": "wfr.not_exist",
            "dataset": "blah"
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example rnasum.by_umccrise_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_by_umccrise_handler_dataset_null(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaExtendedUnitTests.test_rnasum_by_umccrise_handler_dataset_null
        """

        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()

        results = rnasum.by_umccrise_handler(event={
            "wfr_id": mock_umccrise_workflow.wfr_id,
            "dataset": None
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example rnasum.by_umccrise_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_by_subject_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaExtendedUnitTests.test_rnasum_by_subject_handler
        """
        self.verify_local()

        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        results = rnasum.by_subject_handler(event={
            "subject_id": TestConstant.subject_id.value,
            "dataset": "BLCA"
        }, context=None)

        submitted_dataset = results['job_list'][0]['dataset']
        self.assertEqual(submitted_dataset, "BLCA")

        logger.info("-" * 32)
        logger.info("Example rnasum.by_subject_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_by_subject_handler_no_umccrise(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaExtendedUnitTests.test_rnasum_by_subject_handler_no_umccrise
        """
        self.verify_local()

        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()

        results = rnasum.by_subject_handler(event={
            "subject_id": TestConstant.subject_id.value,
            "dataset": "BLCA"
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example rnasum.by_subject_handler lambda output:")
        logger.info(json.dumps(results))

    def test_rnasum_by_subject_handler_ongoing_run(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_rnasum.RNAsumLambdaExtendedUnitTests.test_rnasum_by_subject_handler_ongoing_run
        """
        self.verify_local()

        mock_umccrise_workflow: Workflow = test_rnasum_step.build_mock()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        mock_tumor_wts_library_run = LibraryRun.objects.get(library_id=TestConstant.wts_library_id_tumor.value)

        mock_ongoing_rnasum_run = RNAsumWorkflowFactory()
        mock_ongoing_rnasum_run.libraryrun_set.add(mock_tumor_wts_library_run)
        mock_ongoing_rnasum_run.save()

        results = rnasum.by_subject_handler(event={
            "subject_id": TestConstant.subject_id.value,
            "dataset": "BLCA"
        }, context=None)

        self.assertIn("error", results.keys())

        logger.info("-" * 32)
        logger.info("Example rnasum.by_subject_handler lambda output:")
        logger.info(json.dumps(results))
