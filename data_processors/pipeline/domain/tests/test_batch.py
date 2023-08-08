from data_portal.models import Workflow, FastqListRow, Batch, BatchRun
from data_portal.tests.factories import WorkflowFactory
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.services import batch_srv, fastq_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class BatcherUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(BatcherUnitTests, self).setUp()

    def test_batching(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_batch.BatcherUnitTests.test_batching
        """
        mock_bcl_convert: Workflow = WorkflowFactory()
        mock_bcl_convert.end_status = WorkflowStatus.SUCCEEDED.value
        mock_bcl_convert.save()

        mock_sqr = mock_bcl_convert.sequence_run

        mock_fqlr = FastqListRow()
        mock_fqlr.rgid = f"TCCGGAGA.AGGATAGG.1.{mock_sqr.name}.PRJ123456_L1234567"
        mock_fqlr.rglb = "L1234567"
        mock_fqlr.rgsm = "PRJ123456"
        mock_fqlr.lane = 1
        mock_fqlr.read_1 = f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R1_001.fastq.gz"
        mock_fqlr.read_2 = f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R2_001.fastq.gz"
        mock_fqlr.sequence_run = mock_sqr
        mock_fqlr.save()

        mock_batcher = Batcher(
            workflow=mock_bcl_convert,
            run_step=WorkflowType.DRAGEN_WGTS_QC.value,
            batch_srv=batch_srv,
            fastq_srv=fastq_srv,
            logger=logger,
        )

        self.assertIsNotNone(mock_batcher)
        logger.info(mock_batcher.get_status())
        logger.info(mock_batcher.batch.context_data)

        self.assertEqual(1, Batch.objects.count())
        self.assertEqual(1, BatchRun.objects.count())
