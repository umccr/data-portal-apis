import json
from datetime import datetime

from django.utils.timezone import make_aware
from libica.openapi import libwes
from mockito import when

from data_portal.models.workflow import Workflow
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.libraryrun import LibraryRun
from data_portal.tests.factories import SequenceRunFactory, TestConstant, LibraryRunFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.lambdas import dragen_tso_ctdna
from data_processors.pipeline.services import metadata_srv, workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase
from utils import libjson


class DragenTsoCtDnaTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_tso_ctdna.DragenTsoCtDnaTests.test_handler
        """
        mock_library_run: LibraryRun = LibraryRunFactory()
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ00001")

        workflow: dict = dragen_tso_ctdna.handler({
            "tso500_sample": {
                "sample_id": "PRJ000001_L0000001",
                "sample_name": "sample_name",
                "sample_type": "DNA",
                "pair_id": "sample_name"
            },
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
            ],
            "samplesheet": {
                "class": "File",
                "location": "gds://path/to/samplesheet"
            },
            "run_info_xml": {
                "class": "File",
                "location": "path/to/dir/RunInfo.xml"
            },
            "run_parameters_xml": {
                "class": "File",
                "location": "path/to/dir/RunParameters.xml"
            },
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
            "library_id": TestConstant.library_id_normal.value,
        }, None)

        logger.info("-" * 32)
        logger.info("Example dragen_tso_ctdna.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_TSO_CTDNA_QC workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        for lib_run in all_lib_runs:
            logger.info(lib_run)
            self.assertEqual(lib_run.library_id, TestConstant.library_id_normal.value)

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_tso_ctdna.DragenTsoCtDnaTests.test_handler_alt
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

        workflow: dict = dragen_tso_ctdna.handler({
            "tso500_sample": {
                "sample_id": "sample_id",
                "sample_name": "sample_name",
                "sample_type": "DNA",
                "pair_id": "sample_name"
            },
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
            "samplesheet": {
                "class": "File",
                "location": "gds://path/to/samplesheet"
            },
            "run_info_xml": {
                "class": "File",
                "location": "path/to/dir/RunInfo.xml"
            },
            "run_parameters_xml": {
                "class": "File",
                "location": "path/to/dir/RunParameters.xml"
            },
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
            "library_id": "sample_library",
        }, None)
        logger.info("-" * 32)
        logger.info("Example dragen_tso_ctdna.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_TSO_CTDNA_QC workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_sqs_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_tso_ctdna.DragenTsoCtDnaTests.test_sqs_handler
        """

        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ0001")

        mock_job = {
            "tso500_sample": {
                "sample_id": "sample_id",
                "sample_name": "sample_name",
                "sample_type": "DNA",
                "pair_id": "sample_name"
            },
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
            "samplesheet": {
                "class": "File",
                "location": "gds://path/to/samplesheet"
            },
            "run_info_xml": {
                "class": "File",
                "location": "path/to/dir/RunInfo.xml"
            },
            "run_parameters_xml": {
                "class": "File",
                "location": "path/to/dir/RunParameters.xml"
            },
            "seq_run_id": mock_sqr.run_id,
            "seq_name": mock_sqr.name,
            "batch_run_id": 1,
            "library_id": "sample_library",
        }

        mock_event = {
            'Records': [
                {
                    'messageId': "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                    'body': libjson.dumps(mock_job),
                    'messageAttributes': {},
                    'md5OfBody': "e4e68fb7bd0e697a0ae8f1bb342846b3",
                    'eventSource': "aws:sqs",
                    'eventSourceARN': "arn:aws:sqs:us-east-2:123456789012:fifo.fifo",
                },
            ]
        }

        results = dragen_tso_ctdna.sqs_handler(mock_event, None)
        logger.info("-" * 32)
        logger.info("Example dragen_tso_ctdna.sqs_handler lambda output:")
        logger.info(json.dumps(results))

        self.assertEqual(len(results), 1)


class DragenTsoCtDnaIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
