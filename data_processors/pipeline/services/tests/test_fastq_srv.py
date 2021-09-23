from typing import List

from data_portal.models import FastqListRow, SequenceRun, LabMetadata
from data_portal.tests import factories
from data_processors.pipeline.services import fastq_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase
from utils import libjson

from data_portal.tests.factories import TestConstant
from data_processors.pipeline.services import metadata_srv

class FastqSrvUnitTests(PipelineUnitTestCase):
    mock_library_id = "132"
    mock_sample_id = "1"
    mock_rgms_1 = "1"
    mock_project_owner = "MyMockOwner"

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

    def test_get_fastq_list_row_by_project_owner(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_fastq_srv.FastqSrvUnitTests.test_get_fastq_list_row_by_project_owner
        """
        mock_meta: LabMetadata = factories.LabMetadataFactory()
        mock_sqr: SequenceRun = factories.SequenceRunFactory()

        meta = metadata_srv.get_metadata_by_library_id(TestConstant.library_id_normal.value)

        mock_fqlr = FastqListRow()
        mock_fqlr.rgid = f"TCCGGAGA.AGGATAGG.1.{mock_sqr.name}.PRJ123456_L1234567"
        mock_fqlr.rglb = "L1234567"
        mock_fqlr.rgsm = "PRJ123456"
        mock_fqlr.lane = 1
        mock_fqlr.read_1 = f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R1_001.fastq.gz"
        mock_fqlr.read_2 = f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R2_001.fastq.gz"
        mock_fqlr.sequence_run = mock_sqr
        mock_fqlr.lab_metadata = meta
        mock_fqlr.save()

        fqlr_list = fastq_srv.get_fastq_list_row_by_project_owner(mock_meta.project_owner)
        logger.info(libjson.dumps(fqlr_list))
        self.assertTrue(isinstance(fqlr_list, List))
        self.assertTrue(isinstance(fqlr_list[0]['read_1'], str))  # assert that it is not transformed CWL File object

    def create_lab_meta(self):
        mock_lab_meta: LabMetadata = LabMetadata(
            library_id=self.mock_library_id,
            sample_id=self.mock_sample_id,
            sample_name=self.mock_rgms_1,
            phenotype="tumor",
            quality="good",
            source="tissue",
            type="WGS",
            assay="TsqNano",
            project_owner=self.mock_project_owner,
        )
        mock_lab_meta.save()
        return mock_lab_meta

    def test_create_fastq_list_row_and_fetch_by_project_owner(self): #todo u also ned a WRITE one
        """
        python manage.py test data_processors.pipeline.services.tests.test_fastq_srv.FastqSrvUnitTests.test_create_fastq_list_row_and_fetch_by_project_owner
        """
        mock_sqr: SequenceRun = factories.SequenceRunFactory()

        # create a lab meta with mock_library_id
        labmeta = self.create_lab_meta()

        # create a fqlr with the correct labmeta - service should autolink projectowner
        mock_fqlr_dict = {
            "rgid" : f"TCCGGAGA.AGGATAGG.1.{mock_sqr.name}.PRJ123456_L1234567",
            "rglb" : self.mock_library_id,
            "rgsm" : "PRJ123456",
            "lane" : 1,
            "read_1" : f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R1_001.fastq.gz",
            "read_2" : f"gds://umccr-fastq-data/{mock_sqr.name}/A/UMCCR/PRJ123456_L1234567_S1_L001_R2_001.fastq.gz"
        }
        mock_fqlr = fastq_srv.create_or_update_fastq_list_row(mock_fqlr_dict,mock_sqr)

        fqlr_list = fastq_srv.get_fastq_list_row_by_project_owner(self.mock_project_owner)

        logger.info(libjson.dumps(fqlr_list))
        logger.info(labmeta)
        self.assertTrue(fqlr_list[0]['project_owner'] == labmeta.project_owner)
        self.assertTrue(fqlr_list[0]['rglb'] == self.mock_library_id)
        self.assertTrue(isinstance(fqlr_list, List))
        self.assertTrue(isinstance(fqlr_list[0]['read_1'], str))  # assert that it is not transformed CWL File object    