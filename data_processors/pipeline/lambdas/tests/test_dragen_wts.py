import json
from datetime import datetime

from django.utils.timezone import make_aware
from libica.openapi import libwes
from libumccr import libjson
from mockito import when

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import SequenceRunFactory, TestConstant, WtsTumorLibraryRunFactory, LibraryRunFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType, SecondaryAnalysisHelper
from data_processors.pipeline.lambdas import dragen_wts
from data_processors.pipeline.lambdas.dragen_wts import override_arriba_fusion_step_resources, ARRIBA_FUSION_STEP_KEY_ID
from data_processors.pipeline.services import metadata_srv, workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class DragenWtsUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wts.DragenWtsUnitTests.test_handler
        """
        mock_library_run: LibraryRun = LibraryRunFactory()
        mock_tumor_library_run: LibraryRun = WtsTumorLibraryRunFactory()
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ00001")

        workflow: dict = dragen_wts.handler({
            "subject_id": TestConstant.subject_id.value,
            "library_id": TestConstant.library_id_normal.value,
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": TestConstant.library_id_normal.value,
                    "lane": 1,
                    "read_1": {
                      "class": "File",
                      "location": "gds://path/to/read_1.fastq.gz"
                    },
                    "read_2": {
                      "class": "File",
                      "location": "gds://path/to/read_2.fastq.gz"
                    }
                }
            ]
        }, None)

        logger.info("-" * 32)
        logger.info("Example dragen_wts.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WTS workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())
        self.assertEqual(WorkflowType.DRAGEN_WTS.value, qs.get().type_name)

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        for lib_run in all_lib_runs:
            logger.info(lib_run)
            self.assertEqual(lib_run.library_id, TestConstant.library_id_normal.value)

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wts.DragenWtsUnitTests.test_handler_alt
        """
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ00001")

        mock_wfr: libwes.WorkflowRun = libwes.WorkflowRun()
        mock_wfr.id = TestConstant.wfr_id.value
        mock_wfr.time_started = make_aware(datetime.utcnow())
        mock_wfr.status = WorkflowStatus.RUNNING.value
        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfr.workflow_version = workflow_version
        when(libwes.WorkflowVersionsApi).launch_workflow_version(...).thenReturn(mock_wfr)

        workflow: dict = dragen_wts.handler({
            "subject_id": TestConstant.subject_id.value,
            "library_id": TestConstant.library_id_normal.value,
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": 1,
                    "read_1": {
                        "class": "File",
                        "location": "gds://path/to/read_1.fastq.gz"
                    },
                    "read_2": {
                        "class": "File",
                        "location": "gds://path/to/read_2.fastq.gz"
                    }
                }
            ],
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
        }, None)
        logger.info("-" * 32)
        logger.info("Example dragen_wts.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WTS workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())
        self.assertEqual(WorkflowType.DRAGEN_WTS.value, qs.get().type_name)

    def test_sqs_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wts.DragenWtsUnitTests.test_sqs_handler
        """

        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ00001")

        mock_job = {
            "subject_id": "SUBJECT_ID",
            "library_id": "SAMPLE_NAME",
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "sample_library",
                    "lane": 1,
                    "read_1": {
                        "class": "File",
                        "location": "gds://path/to/read_1.fastq.gz"
                    },
                    "read_2": {
                        "class": "File",
                        "location": "gds://path/to/read_2.fastq.gz"
                    }
                }
            ],
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
            "batch_run_id": 1
        }

        mock_event = {
            'Records': [
                {
                    'messageId': "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                    'body': libjson.dumps(mock_job),
                    'messageAttributes': {},
                    'md5OfBody': "",
                    'eventSource': "aws:sqs",
                    'eventSourceARN': "arn:aws:sqs:us-east-2:123456789012:fifo.fifo",
                },
            ]
        }

        results = dragen_wts.sqs_handler(mock_event, None)
        logger.info("-" * 32)
        logger.info("Example dragen_wts.sqs_handler lambda output:")
        logger.info(json.dumps(results))

        self.assertEqual(len(results), 2)

    def test_override_arriba_fusion_step_resources(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wts.DragenWtsUnitTests.test_override_arriba_fusion_step_resources
        :return:
        """
        helper = SecondaryAnalysisHelper(WorkflowType.DRAGEN_WTS)
        eng_params = helper.get_engine_parameters(target_id="SBJ0002", secondary_target_id=None)

        eng_params = override_arriba_fusion_step_resources(eng_params)

        logger.info("New engine parameters are as follows:")
        logger.info("\n" + json.dumps(eng_params, indent=2))

        self.assertIn("overrides", eng_params)
        self.assertIn(ARRIBA_FUSION_STEP_KEY_ID, eng_params.get("overrides"))
        self.assertIn("requirements", eng_params.get("overrides").get(ARRIBA_FUSION_STEP_KEY_ID))
        self.assertIn("ResourceRequirement", eng_params.get("overrides").get(ARRIBA_FUSION_STEP_KEY_ID).get("requirements"))
        self.assertEqual(
            {
                "https://platform.illumina.com/rdf/ica/resources": {
                    "size": "medium",
                    "type": "standardHiMem"
                }
            },
            eng_params.get("overrides").get(ARRIBA_FUSION_STEP_KEY_ID).get("requirements").get("ResourceRequirement")
        )


class DragenWtsIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit ICA endpoints
    pass
