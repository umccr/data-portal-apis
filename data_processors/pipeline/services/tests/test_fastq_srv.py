from typing import List

from data_portal.models import FastqListRow, SequenceRun
from data_portal.tests import factories
from data_processors.pipeline.services import fastq_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase
from utils import libjson


class FastqSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(FastqSrvUnitTests, self).setUp()

    def test_get_fastq_list_row_by_sequence_name(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_fastq_srv.FastqSrvUnitTests.test_get_fastq_list_row_by_sequence_name
        """

        mock_sqr: SequenceRun = factories.SequenceRunFactory()

        mock_fqlr = FastqListRow()
        mock_fqlr.rgid = f"TCCGGAGA.AGGATAGG.1.{mock_sqr.name}.PRJ123456_L1234567"
        mock_fqlr.rglb = "L1234567"
        mock_fqlr.rgsm = "PRJ123456"
        mock_fqlr.lane = 1
        mock_fqlr.read_1 = f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R1_001.fastq.gz"
        mock_fqlr.read_2 = f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R2_001.fastq.gz"
        mock_fqlr.sequence_run = mock_sqr
        mock_fqlr.save()

        fqlr_list = fastq_srv.get_fastq_list_row_by_sequence_name(mock_sqr.name)
        logger.info(libjson.dumps(fqlr_list))
        self.assertTrue(isinstance(fqlr_list, List))
        self.assertTrue(isinstance(fqlr_list[0]['read_1'], str))  # assert that it is not transformed CWL File object
