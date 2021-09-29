import logging

from django.db import IntegrityError
from django.test import TestCase

from data_portal.models import LabMetadata,FastqListRow,FastqListRowManager, SequenceRun
from data_portal.tests.factories import LabMetadataFactory, SequenceRunFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FastqListRowTests(TestCase):

    def test_retrieve_by_project_owner(self):
        # create a lab metadata and a FastqListRow and 

        mock_lib_id = "ABC123"
        mock_project_owner = "A Programmer"
        lm: LabMetadata = LabMetadata(
            library_id=mock_lib_id,
            sample_id="foobar",
            sample_name="foobar",
            phenotype="tumor",
            quality="good",
            source="tissue",
            type="WGS",
            assay="TsqNano",
            project_owner=mock_project_owner)              
        lm.save()

        sequence_run: SequenceRun = SequenceRunFactory()

        FastqListRow.objects.create_fastq_list_row("rg"+mock_lib_id,'rgsm',mock_lib_id,'1','read_1','read_2',sequence_run,None)      

        results = FastqListRow.objects.get_by_keyword(project_owner=mock_project_owner)
        
        # this ought to autolink to the labmetadata
        logger.info(results)
        self.assertEqual(results.count(),1)
