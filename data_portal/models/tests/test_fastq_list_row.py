import logging

from django.test import TestCase

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.sequencerun import SequenceRun
from data_portal.tests.factories import SequenceRunFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FastqListRowTests(TestCase):

    def test_retrieve_by_project_owner(self):
        """
        python manage.py test data_portal.tests.test_fastq_list_row.FastqListRowTests.test_retrieve_by_project_owner
        """

        mock_lib_id = "L1234567"
        mock_project_owner = "Scott"

        mock_meta: LabMetadata = LabMetadata(
            library_id=mock_lib_id,
            sample_id="foobar",
            sample_name="foobar",
            phenotype="tumor",
            quality="good",
            source="tissue",
            type="WGS",
            assay="TsqNano",
            project_owner=mock_project_owner)
        mock_meta.save()

        mock_sequence_run: SequenceRun = SequenceRunFactory()

        mock_fqlr = FastqListRow(
            rgid=f"CTCAGAAG.AACTTGCC.4.210923_A00130_0001_BHH5JFDSX2.PRJ123456_{mock_lib_id}",
            rgsm="PRJ123456",
            rglb=mock_lib_id,
            lane=1,
            read_1="gds://volume/path_R1.fastq.gz",
            read_2="gds://volume/path_R2.fastq.gz",
            sequence_run=mock_sequence_run
        )
        mock_fqlr.save()

        results = FastqListRow.objects.get_by_keyword(
            run=mock_sequence_run.instrument_run_id,
            project_owner=mock_project_owner
        )

        self.assertEqual(results.count(), 1)

        fqlr = results.get()
        logger.info(fqlr)
        self.assertEqual(fqlr.rglb, mock_lib_id)
