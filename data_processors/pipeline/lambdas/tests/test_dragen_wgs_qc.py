import json
from datetime import datetime

from django.utils.timezone import make_aware
from libica.openapi import libwes
from libumccr import libjson
from mockito import when

from data_portal.models import LabMetadata
from data_portal.models.labmetadata import LabMetadataType
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import SequenceRunFactory, TestConstant, LibraryRunFactory, LabMetadataFactory, \
    WtsTumorLabMetadataFactory, WtsTumorLibraryRunFactory
from data_processors.pipeline.domain.workflow import SecondaryAnalysisHelper, WorkflowType
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.lambdas import dragen_wgs_qc
from data_processors.pipeline.services import metadata_srv, workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class DragenWgsQcUnitTests(PipelineUnitTestCase):

    def test_handler_no_meta(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler_no_meta
        """

        with self.assertRaises(ValueError) as cm:
            dragen_wgs_qc.handler({
                "library_id": TestConstant.library_id_normal.value,
                "lane": TestConstant.lane_normal_library.value,
                "fastq_list_rows": [
                    {
                        "rgid": "index1.index2.lane",
                        "rgsm": "sample_name",
                        "rglb": "L0000001",
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
            }, None)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Metadata not found", str(e))

    def test_handler_wrong_meta_type(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler_wrong_meta_type
        """

        mock_normal_library: LabMetadata = LabMetadataFactory()
        mock_normal_library.type = LabMetadataType.EXOME
        mock_normal_library.save()

        with self.assertRaises(ValueError) as cm:
            dragen_wgs_qc.handler({
                "library_id": TestConstant.library_id_normal.value,
                "lane": TestConstant.lane_normal_library.value,
                "fastq_list_rows": [
                    {
                        "rgid": "index1.index2.lane",
                        "rgsm": "sample_name",
                        "rglb": "L0000001",
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
            }, None)
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("be one of WGS or WTS", str(e))

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler
        """
        mock_normal_library = LabMetadataFactory()
        mock_library_run: LibraryRun = LibraryRunFactory()
        mock_sqr: SequenceRun = SequenceRunFactory()

        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ00001")

        workflow: dict = dragen_wgs_qc.handler({
            "library_id": TestConstant.library_id_normal.value,
            "lane": TestConstant.lane_normal_library.value,
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "L0000001",
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
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WGS_QC workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        for lib_run in workflow_srv.get_all_library_runs_by_workflow(wfl):
            logger.info(lib_run)
            self.assertEqual(lib_run.library_id, TestConstant.library_id_normal.value)

    def test_handler_wts(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler_wts
        """
        mock_wts_library: LabMetadata = WtsTumorLabMetadataFactory()
        mock_wts_library_run: LibraryRun = WtsTumorLibraryRunFactory()
        mock_sqr: SequenceRun = SequenceRunFactory()

        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn(TestConstant.subject_id.value)

        workflow: dict = dragen_wgs_qc.handler({
            "library_id": mock_wts_library.library_id,
            "lane": TestConstant.wts_lane_tumor_library.value,
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "L0000001",
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
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WTS_QC workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        wfl = qs.get()

        # assert SecondaryAnalysisHelper initialise with proper workflow type `wts_alignment_qc`
        self.assertEqual(wfl.type_name, WorkflowType.DRAGEN_WTS_QC.value)  # assert db record
        self.assertIn(WorkflowType.DRAGEN_WTS_QC.value, wfl.wfr_name)

        # assert that we can query related LibraryRun from Workflow side
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(1, len(all_lib_runs))
        for lib_run in workflow_srv.get_all_library_runs_by_workflow(wfl):
            logger.info(lib_run)
            self.assertEqual(lib_run.library_id, TestConstant.wts_library_id_tumor.value)

    def test_handler_alt(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_handler_alt
        """
        mock_normal_library = LabMetadataFactory()
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

        workflow: dict = dragen_wgs_qc.handler({
            "library_id": TestConstant.library_id_normal.value,
            "lane": 1,
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
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WGS_QC workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())

    def test_sqs_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_sqs_handler
        """
        mock_normal_library = LabMetadataFactory()
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ0001")

        mock_job = {
            "library_id": TestConstant.library_id_normal.value,
            "lane": 1,
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

        results = dragen_wgs_qc.sqs_handler(mock_event, None)
        logger.info("-" * 32)
        logger.info("Example dragen_wgs_qc.sqs_handler lambda output:")
        logger.info(json.dumps(results))

        # expecting length of 2 in results
        # i.e. one for the handler results and one for the batchItemFailures
        # No matter what, both should always be present.
        self.assertEqual(len(results), 2)

    def test_portal_run_id(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_dragen_wgs_qc.DragenWgsQcUnitTests.test_portal_run_id
        """
        mock_portal_run_id = "20211101a1b2c3d4"
        mock_normal_library = LabMetadataFactory()
        LibraryRunFactory()
        mock_sqr: SequenceRun = SequenceRunFactory()
        when(metadata_srv).get_subject_id_from_library_id(...).thenReturn("SBJ00001")
        when(SecondaryAnalysisHelper).get_portal_run_id().thenReturn(mock_portal_run_id)

        workflow: dict = dragen_wgs_qc.handler({
            "library_id": TestConstant.library_id_normal.value,
            "lane": TestConstant.lane_normal_library.value,
            "fastq_list_rows": [
                {
                    "rgid": "index1.index2.lane",
                    "rgsm": "sample_name",
                    "rglb": "L0000001",
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
        logger.info("Example dragen_wgs_qc.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert DRAGEN_WGS_QC workflow launch success and save workflow run in db
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl: Workflow = qs.get()
        self.assertEqual(mock_portal_run_id, wfl.portal_run_id)


class DragenWgsQcIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
