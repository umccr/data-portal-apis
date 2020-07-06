from libiap.openapi import libgds
from mockito import when

from data_portal.models import SequenceRun, Workflow
from data_portal.tests.factories import SequenceRunFactory
from data_processors.pipeline.lambdas import demux
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class DemuxUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux.DemuxUnitTests.test_handler
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_file_list: libgds.FileListResponse = libgds.FileListResponse()
        mock_file_list.items = [
            libgds.FileResponse(name="NA12345 - 4KC_S7_R1_001.fastq.gz"),
            libgds.FileResponse(name="NA12345 - 4KC_S7_R2_001.fastq.gz"),
            libgds.FileResponse(name="PRJ111119_L1900000_S1_R1_001.fastq.gz"),
            libgds.FileResponse(name="PRJ111119_L1900000_S1_R2_001.fastq.gz"),
            libgds.FileResponse(name="MDX199999_L1999999_topup_S2_R1_001.fastq.gz"),
            libgds.FileResponse(name="MDX199999_L1999999_topup_S2_R2_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R1_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R2_001.fastq.gz"),
        ]
        when(libgds.FilesApi).list_files(...).thenReturn(mock_file_list)

        workflow_json = demux.handler({
            'workflow_type': "germline",
            'gds_path': "gds://volume/path/to/fastq",
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("")
        logger.info("Example demux.handler lambda output:")
        logger.info(workflow_json)

        # assert demux germline workflows launch success and save workflow runs in db
        workflows = Workflow.objects.all()
        self.assertEqual(4, workflows.count())

    def test_handler_unsupported_workflow(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux.DemuxUnitTests.test_handler_unsupported_workflow
        """

        workflow_json = demux.handler({
            'workflow_type': "something_else",
            'gds_path': "gds://volume/path/to/fastq",
            'seq_run_id': "sequence run id",
            'seq_name': "sequence run name",
        }, None)

        logger.info("")
        logger.info("Example demux.handler lambda output:")
        logger.info(workflow_json)

        # assert no demux workflows launch and no workflow runs save in db
        workflows = Workflow.objects.all()
        self.assertEqual(0, workflows.count())

    def test_handler_paired_end(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux.DemuxUnitTests.test_handler_paired_end
        """
        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_file_list: libgds.FileListResponse = libgds.FileListResponse()
        mock_file_list.items = [
            libgds.FileResponse(name="NA12345_S7_L001_R1_001.fastq.gz"),
            libgds.FileResponse(name="NA12345_S7_L002_R1_001.fastq.gz"),
            libgds.FileResponse(name="NA12345_S7_L001_R2_001.fastq.gz"),
            libgds.FileResponse(name="NA12345_S7_L002_R2_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R1_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R2_001.fastq.gz"),
        ]
        when(libgds.FilesApi).list_files(...).thenReturn(mock_file_list)

        workflow_json = demux.handler({
            'workflow_type': "germline",
            'gds_path': "gds://volume/path/to/fastq",
            'seq_run_id': mock_sqr.run_id,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("")
        logger.info("Example demux.handler lambda output:")
        logger.info(workflow_json)

        # assert demux germline workflows launch success and save workflow runs in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())


class DemuxIntegrationTests(PipelineIntegrationTestCase):
    # write test case here if to actually hit IAP endpoints
    pass
