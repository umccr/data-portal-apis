import json
import tempfile
from io import BytesIO
from typing import List, Dict
from unittest import skip
from mockito import when
import pandas as pd
import numpy as np

from data_portal.models import LabMetadata
from data_processors.lims.lambdas import labmetadata
from data_processors.lims.services import persist_labmetadata
from data_processors.lims.tests.case import LimsUnitTestCase, LimsIntegrationTestCase, logger

# columns in LIMS CSV
labmetadata_csv_columns = [
        'LibraryID','SampleName','SampleID','ExternalSampleID','SubjectID','ExternalSubjectID','Phenotype','Quality','Source','ProjectName','ProjectOwner','','ExperimentID','Type','Assay','OverrideCycles','Workflow','Coverage (X)','TruSeq Index, unless stated','Run#','Comments','rRNA','qPCR ID','Sample_ID (SampleSheet)'
]

_mock_labmetadata_sheet_content = b"""
LibraryID,SampleName,SampleID,ExternalSampleID,SubjectID,ExternalSubjectID,Phenotype,Quality,Source,ProjectName,ProjectOwner,,ExperimentID,Type,Assay,OverrideCycles,Workflow,Coverage (X),"TruSeq Index, unless stated",Run#,Comments,rRNA,qPCR ID,Sample_ID (SampleSheet),,,,,,,,,,
LIB01,SAMIDA-EXTSAMA,SAMIDA,,SUBIDA,EXTSUBIDA,,,FFPE,MyPath,Alice,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-F07,P30,,,#NAME?,SAMIDA_LIB01,,,,,,,,,,
LIB02,SAMIDB-EXTSAMB,SAMIDB,EXTSAMB,SUBIDB,EXTSUBIDB,tumor,poor,FFPE,Fake,Bob,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-G07,P30,,,#NAME?,SAMIDB_LIB02,,,,,,,,,,
LIB03,SAMIDB-EXTSAMB,SAMIDB,EXTSAMB,SUBIDB,EXTSUBIDB,tumor,poor,FFPE,Fake,Bob,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-H07,P30,,,#NAME?,SAMIDB_LIB03,,,,,,,,,,
LIB04,SAMIDA-EXTSAMA,SAMIDA,EXTSAMA,SUBIDA,EXTSUBIDA,tumor,poor,FFPE,MyPath,Alice,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-F07,P30,,,#NAME?,SAMIDA_LIB01,,,,,,,,,,
"""


def _generate_labmetadata_row_dict(id: str) -> dict:
    """
    Generate labmetadata row dict
    :param id: this just gets used asa suffix
    :return: row dict
    """

    # 'clean' CSV columns into django model compatible columns
    df = pd.DataFrame(columns=labmetadata_csv_columns)
    cleaned_labmetadata_csv_columns = list(labmetadata.clean_labmetadata_dataframe_columns(df).columns.values)

    row = dict()
    for col in cleaned_labmetadata_csv_columns:
        if col == 'run':
            row[col] = '1'
        elif col == 'timestamp':
            row[col] = '2019-01-01'
        else:
            # Normal columns, just use column name as value + id
            row[col] = col + id
    return row


def _generate_labmetadata_df(rows: List[Dict[str, str]]) -> pd.DataFrame:
    df = pd.DataFrame(columns=labmetadata_csv_columns)
    df = labmetadata.clean_labmetadata_dataframe_columns(df)

    for row in rows:
        df = df.append(row,ignore_index=True)
    return df


class LimsUnitTests(LimsUnitTestCase):

    def setUp(self) -> None:
        super(LimsUnitTests, self).setUp()

    def tearDown(self) -> None:
        super(LimsUnitTests, self).tearDown()  # parent tear down should call last

    def test_scheduled_update_handler(self):
        mock_labmetadata_sheet = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)  # delete=False keep file in tmp dir
        mock_labmetadata_sheet.write(_mock_labmetadata_sheet_content.lstrip().rstrip())
        mock_labmetadata_sheet.seek(0)
        mock_labmetadata_sheet.flush()
        
        # make a duplicate , its phenotype is normal, in sheet it is tumor
        test_existing_sample=LabMetadata.objects.create(library_id='LIB03',sample_name='SAMIDB-EXTSAMB',sample_id='SAMIDB',external_sample_id='EXTSAMB',subject_id='SUBIDB',external_subject_id='EXTSUBIDB',phenotype='NORMAL',quality='poor',source='FFPE',project_name='Fake',project_owner='Bob',experiment_id='Exper1',type='WTS',assay='NebRNA',override_cycles='Y151;I8;I8;Y151',workflow='clinical',coverage='6.0',truseqindex='H07')

        # print csv file in tmp dir -- if delete=False, you can see the mock csv content
        logger.info(f"Path to mock tracking sheet: {mock_labmetadata_sheet.name}")

        when(labmetadata.libgdrive).download_sheet(...).thenReturn(pd.read_csv(mock_labmetadata_sheet))

        result = labmetadata.scheduled_update_handler({'event': "mock lims update event"}, None)

        logger.info("-" * 32)
        logger.info("Example labmetadata.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertEqual(result['labmetadata_row_new_count'], 3)
        self.assertEqual(result['labmetadata_row_update_count'], 1)
        libBlankExtSampleId = LabMetadata.objects.get(library_id='LIB01')
        self.assertEqual(libBlankExtSampleId.external_sample_id,'')
        libCreated = LabMetadata.objects.get(library_id='LIB02')
        self.assertIsNotNone(libCreated)
        libUpdated = LabMetadata.objects.get(library_id='LIB03')
        self.assertEqual(libUpdated.phenotype,'tumor')

        # clean up
        mock_labmetadata_sheet.close()
 
    def test_labmetadata_rewrite(self) -> None:
        sample_name = 'sample_name'
        library_id = 'library_id'

        row_1 = _generate_labmetadata_row_dict('1')
        row_1['sample_name'] = sample_name
        row_1['library_id'] = library_id
        process_results = persist_labmetadata(_generate_labmetadata_df([row_1]))

        self.assertEqual(process_results['labmetadata_row_new_count'], 1)
        self.assertEqual(LabMetadata.objects.get(subject_id='subject_id1', sample_name=sample_name).external_sample_id, row_1['external_sample_id'])
        self.assertEqual(process_results['labmetadata_row_update_count'], 0)
        self.assertEqual(process_results['labmetadata_row_invalid_count'], 0)

   
    def test_labmetadata_update(self) -> None:
        row_1 = _generate_labmetadata_row_dict('1')
        persist_labmetadata(_generate_labmetadata_df([row_1]))

        new_assay = 'new_assay'
        row_1['assay'] = new_assay
        row_2 = _generate_labmetadata_row_dict('2')
        process_results = persist_labmetadata(_generate_labmetadata_df([row_1,row_2]))

        self.assertEqual(process_results['labmetadata_row_new_count'], 1)
        self.assertEqual(process_results['labmetadata_row_update_count'], 1)
        self.assertEqual(process_results['labmetadata_row_invalid_count'], 0)
        self.assertEqual(LabMetadata.objects.get(library_id='library_id1', sample_name='sample_name1').assay, new_assay)

    def test_labmetadata_row_duplicate(self) -> None:
        row_duplicate = _generate_labmetadata_row_dict('3')
        process_results = persist_labmetadata(_generate_labmetadata_df([row_duplicate,row_duplicate]))
        logger.info(json.dumps(process_results))
        self.assertEqual(process_results['labmetadata_row_update_count'], 0)
        self.assertEqual(process_results['labmetadata_row_new_count'], 1)
        self.assertEqual(process_results['labmetadata_row_invalid_count'], 1)

    def test_labmetadata_non_nullable_columns(self) -> None:
        row_1 = _generate_labmetadata_row_dict('1')
        row_2 = _generate_labmetadata_row_dict('2')

        # Use blank values for all non-nullable fields
        row_1['sample_id'] = ''
        row_1['sample_name'] = ''
        row_1['library_id'] = ''
        # the bad row should be removed from the DF by persist_labmetada to end with 1 new and 1 invalid
        process_results = persist_labmetadata(_generate_labmetadata_df([row_1, row_2]))
        self.assertEqual(process_results['labmetadata_row_update_count'], 0)
        self.assertEqual(process_results['labmetadata_row_new_count'], 1)
        self.assertEqual(process_results['labmetadata_row_invalid_count'], 1)
        self.assertEqual(LabMetadata.objects.count(), 1)

    def test_labmetadata_empty_colums(self) -> None:
        # set a bunch of nullable/empty-able rows 
        row_1 = _generate_labmetadata_row_dict('1')
        row_1['subject_id'] = '-' 
        row_2 = _generate_labmetadata_row_dict('2')
        row_2['subject_id'] = ''  
        row_3 = _generate_labmetadata_row_dict('3')
        row_3['coverage'] = np.nan
        row_4 = _generate_labmetadata_row_dict('4')
        row_4['subject_id'] = ' '

        process_results = persist_labmetadata(_generate_labmetadata_df([row_1,row_2,row_3,row_4]))

        self.assertEqual(LabMetadata.objects.count(), 4)
        self.assertEqual(process_results['labmetadata_row_new_count'], 4)
 


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
