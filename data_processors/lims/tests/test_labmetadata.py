import json
import tempfile
from io import BytesIO
from typing import List, Dict
from unittest import skip

from mockito import when
import pandas as pd

from data_portal.models import LabMetadata
from data_processors.lims.lambdas import labmetadata
from data_processors.lims.services import persist_labmetadata
from data_processors.lims.tests.case import LimsUnitTestCase, LimsIntegrationTestCase, logger

# columns in LIMS CSV
labmetadata_csv_columns = [
    'LibraryID','SampleName','SampleID','ExternalSampleID','SubjectID','ExternalSubjectID','Phenotype','Quality','Source','ProjectName','ProjectOwner','','ExperimentID','Type','Assay','OverrideCycles','Workflow','Coverage (X)','"TruSeq Index',' unless stated"','Run#','Comments','rRNA','qPCR ID','Sample_ID (SampleSheet)'
]

_mock_labmetadata_sheet_content = b"""
LibraryID,SampleName,SampleID,ExternalSampleID,SubjectID,ExternalSubjectID,Phenotype,Quality,Source,ProjectName,ProjectOwner,,ExperimentID,Type,Assay,OverrideCycles,Workflow,Coverage (X),"TruSeq Index, unless stated",Run#,Comments,rRNA,qPCR ID,Sample_ID (SampleSheet),,,,,,,,,,
LIB01,SAMIDA-EXTSAMA,SAMIDA,EXTSAMA,SUBIDA,EXTSUBIDA,tumor,poor,FFPE,MyPath,Alice,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-F07,P30,,,#NAME?,SAMIDA_LIB01,,,,,,,,,,
LIB02,SAMIDB-EXTSAMB,SAMIDB,EXTSAMB,SUBIDB,EXTSUBIDB,tumor,poor,FFPE,Fake,Bob,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-G07,P30,,,#NAME?,SAMIDB_LIB02,,,,,,,,,,
LIB03,SAMIDB-EXTSAMB,SAMIDB,EXTSAMB,SUBIDB,EXTSUBIDB,tumor,poor,FFPE,Fake,Bob,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-H07,P30,,,#NAME?,SAMIDB_LIB03,,,,,,,,,,
"""


def _generate_labmetadata_csv_row_dict(id: str) -> dict:
    """
    Generate labmetadata csv row dict
    :param id: id of the row, to make this row distinguishable
    :return: row dict
    """
    row = dict()
    for col in labmetadata_csv_columns:
        if col == 'Run':
            row[col] = '1'
        elif col == 'Timestamp':
            row[col] = '2019-01-01'
        else:
            # Normal columns, just use column name as value + id
            row[col] = col + id
    return row


def _generate_labmetadata_csv(rows: List[Dict[str, str]]):
    csv_data = ','.join(labmetadata_csv_columns) + '\n'  # Generate header row

    for row in rows:
        csv_data += ','.join(row.values()) + '\n'

    return csv_data


class LimsUnitTests(LimsUnitTestCase):

    def setUp(self) -> None:
        super(LimsUnitTests, self).setUp()

    def tearDown(self) -> None:
        super(LimsUnitTests, self).tearDown()  # parent tear down should call last

    def test_ingest_metadata(self):

        mock_labmetadata_sheet = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)  # delete=False keep file in tmp dir
        mock_labmetadata_sheet.write(_mock_labmetadata_sheet_content.lstrip().rstrip())
        mock_labmetadata_sheet.seek(0)
        mock_labmetadata_sheet.flush()

        # print csv file in tmp dir -- if delete=False, you can see the mock csv content
        logger.info(f"Path to mock lims sheet: {mock_labmetadata_sheet.name}")

        when(labmetadata.libgdrive).download_sheet(...).thenReturn(pd.read_csv(mock_labmetadata_sheet))

        result = labmetadata.scheduled_update_handler({'event': "mock lims update event"}, None)

        logger.info("-" * 32)
        logger.info("Example google_lims.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertEqual(result['labmetadata_row_new_count'], 1)

        sbj = LabMetadata.objects.get(subject_id='SBJ00001')
        logger.info(sbj)
        self.assertIsNotNone(sbj)

        # clean up
        mock_labmetadata_sheet.close()

  


class LimsIntegrationTests(LimsIntegrationTestCase):
    # some test case to hit actual API endpoint
    # annotate @skip to make the test cast to run through manual mean

    @skip
    def test_scheduled_update_handler(self):
        """
        python manage.py test data_processors.lims.tests.test_google_lims.LimsIntegrationTests.test_scheduled_update_handler
        """
        result = labmetadata.scheduled_update_handler({'event': "LimsIntegrationTests lims update event"}, None)

        logger.info("-" * 32)
        logger.info("Example google_lims.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertGreater(result['labmetadata_row_new_count'], 1)

        logger.info(f"Total ingested rows into test db: {LabMetadata.objects.count()}")
