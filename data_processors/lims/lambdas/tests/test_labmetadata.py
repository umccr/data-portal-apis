import json
import tempfile
from typing import List, Dict
from unittest import skip

import numpy as np
import pandas as pd
from django.test import TransactionTestCase
from libumccr import libgdrive
from mockito import when

from data_portal.models.labmetadata import LabMetadata
from data_processors.lims.lambdas import labmetadata
from data_processors.lims.services import labmetadata_srv
from data_processors.lims.tests.case import LimsIntegrationTestCase, logger

labmetadata_csv_columns = [
    'LibraryID', 'SampleName', 'SampleID', 'ExternalSampleID', 'SubjectID', 'ExternalSubjectID', 'Phenotype', 'Quality',
    'Source', 'ProjectName', 'ProjectOwner', '', 'ExperimentID', 'Type', 'Assay', 'OverrideCycles', 'Workflow',
    'Coverage (X)', 'TruSeq Index, unless stated', 'Run#', 'Comments', 'rRNA', 'qPCR ID', 'Sample_ID (SampleSheet)'
]

_mock_labmetadata_sheet_content = b"""
LibraryID,SampleName,SampleID,ExternalSampleID,SubjectID,ExternalSubjectID,Phenotype,Quality,Source,ProjectName,ProjectOwner,,ExperimentID,Type,Assay,OverrideCycles,Workflow,Coverage (X),"TruSeq Index, unless stated",Run#,Comments,rRNA,qPCR ID,Sample_ID (SampleSheet),,,,,,,,,,
LIB01,SAMIDA-EXTSAMA,SAMIDA,,SUBIDA,EXTSUBIDA,,,FFPE,MyPath,Alice,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-F07,P30,,,#NAME?,SAMIDA_LIB01,,,,,,,,,,
LIB02,SAMIDB-EXTSAMB,SAMIDB,EXTSAMB,SUBIDB,EXTSUBIDB,tumor,poor,FFPE,Fake,Bob,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-G07,P30,,,#NAME?,SAMIDB_LIB02,,,,,,,,,,
LIB03,SAMIDB-EXTSAMB,SAMIDB,EXTSAMB,SUBIDB,EXTSUBIDB,tumor,poor,FFPE,Fake,Bob,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-H07,P30,,,#NAME?,SAMIDB_LIB03,,,,,,,,,,
LIB04,SAMIDA-EXTSAMA,SAMIDA,EXTSAMA,SUBIDA,EXTSUBIDA,tumor,poor,FFPE,MyPath,Alice,,Exper1,WTS,NebRNA,Y151;I8;I8;Y151,clinical,6.0,Neb2-F07,P30,,,#NAME?,SAMIDA_LIB01,,,,,,,,,,
"""


def _generate_labmetadata_row_dict(id_: str) -> dict:
    """
    Generate labmetadata row dict
    :param id_: this just gets used as a suffix
    :return: row dict
    """

    # 'clean' CSV columns into columns matching django model  
    df = pd.DataFrame(columns=labmetadata_csv_columns)
    cleaned_labmetadata_csv_columns = list(labmetadata_srv.clean_columns(df).columns.values)

    row = dict()
    for col in cleaned_labmetadata_csv_columns:
        row[col] = col + id_
    return row


def _generate_labmetadata_df(rows: List[Dict[str, str]]) -> pd.DataFrame:
    df = pd.DataFrame(columns=labmetadata_csv_columns)
    df = labmetadata_srv.clean_columns(df)

    for row in rows:
        df = df.append(row, ignore_index=True)

    return df


class LabMetadataUnitTests(TransactionTestCase):

    def setUp(self) -> None:
        super(LabMetadataUnitTests, self).setUp()

    def tearDown(self) -> None:
        super(LabMetadataUnitTests, self).tearDown()  # parent tear down should call last

    def test_scheduled_update_handler(self):
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataUnitTests.test_scheduled_update_handler
        """
        mock_labmetadata_sheet = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)
        mock_labmetadata_sheet.write(_mock_labmetadata_sheet_content.lstrip().rstrip())
        mock_labmetadata_sheet.seek(0)
        mock_labmetadata_sheet.flush()

        # make a duplicate to test update, its phenotype is normal but in sheet it is tumor
        lab_meta = LabMetadata(
            library_id="LIB03",
            sample_name="SAMIDB-EXTSAMB",
            sample_id="SAMIDB",
            external_sample_id="EXTSAMB",
            subject_id="SUBIDB",
            external_subject_id="EXTSUBIDB",
            phenotype="NORMAL",
            quality="poor",
            source="FFPE",
            project_name="Fake",
            project_owner="Bob",
            experiment_id="Exper1",
            type="WTS",
            assay="NebRNA",
            override_cycles="Y151;I8;I8;Y151",
            workflow="clinical",
            coverage="6.0",
            truseqindex="H07"
        )
        lab_meta.save()

        # print csv file in tmp dir -- if delete=False, you can see the mock csv content
        logger.info(f"Path to mock tracking sheet: {mock_labmetadata_sheet.name}")

        when(libgdrive).download_sheet(...).thenReturn(pd.read_csv(mock_labmetadata_sheet))

        result = labmetadata.scheduled_update_handler({
            'sheets': ["2021"],
            'truncate': False,
        }, None)

        logger.info("-" * 32)
        logger.info("Example labmetadata.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))

        self.assertEqual(result['labmetadata_row_new_count'], 3)
        self.assertEqual(result['labmetadata_row_update_count'], 1)
        self.assertEqual(result['labmetadata_row_invalid_count'], 0)

        lib_blank_ext_sample_id = LabMetadata.objects.get(library_id="LIB01")
        self.assertEqual(lib_blank_ext_sample_id.external_sample_id, "")

        lib_created = LabMetadata.objects.get(library_id="LIB02")
        self.assertIsNotNone(lib_created)

        lib_updated = LabMetadata.objects.get(library_id="LIB03")
        self.assertEqual(lib_updated.phenotype, "tumor")

        # clean up
        mock_labmetadata_sheet.close()

    def test_labmetadata_truncate(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataUnitTests.test_labmetadata_truncate
        """
        mock_labmetadata_sheet = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)
        mock_labmetadata_sheet.write(_mock_labmetadata_sheet_content.lstrip().rstrip())
        mock_labmetadata_sheet.seek(0)
        mock_labmetadata_sheet.flush()

        # make some existing data
        for n in range(10):
            meta = LabMetadata(
                library_id=f"L000000{n}",
                sample_name=f"PRJ00000{n}_L000000{n}",
                sample_id=f"PRJ00000{n}"
            )
            meta.save()

        lab_meta = LabMetadata(
            library_id="LIB03",
            sample_name="SAMIDB-EXTSAMB",
            sample_id="SAMIDB",
        )
        lab_meta.save()

        self.assertEqual(11, LabMetadata.objects.count())  # we have existing 11 rows

        # print csv file in tmp dir -- if delete=False, you can see the mock csv content
        logger.info(f"Path to mock tracking sheet: {mock_labmetadata_sheet.name}")

        when(libgdrive).download_sheet(...).thenReturn(pd.read_csv(mock_labmetadata_sheet))

        result = labmetadata.scheduled_update_handler({'sheets': ["2020"]}, None)  # set only to 1 sheet

        logger.info("-" * 32)
        logger.info("Example labmetadata.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))

        self.assertEqual(result['labmetadata_row_new_count'], 4)
        self.assertEqual(result['labmetadata_row_update_count'], 0)  # no update, everything should be re-created!!
        self.assertEqual(result['labmetadata_row_invalid_count'], 0)

        self.assertEqual(4, LabMetadata.objects.count())

        # clean up
        mock_labmetadata_sheet.close()

    def test_labmetadata_update(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataUnitTests.test_labmetadata_update
        """
        row_1 = _generate_labmetadata_row_dict('1')
        mock_df1 = _generate_labmetadata_df([row_1])
        logger.info(f"\n{mock_df1}")
        _ = labmetadata_srv.persist_labmetadata(mock_df1)

        new_assay = 'new_assay'
        row_1['assay'] = new_assay
        row_2 = _generate_labmetadata_row_dict('2')

        mock_df2 = _generate_labmetadata_df([row_1, row_2])
        logger.info(f"\n{mock_df2}")
        result = labmetadata_srv.persist_labmetadata(mock_df2)

        logger.info("-" * 32)
        logger.info(json.dumps(result))

        lab_meta = LabMetadata.objects.get(library_id='library_id1', sample_name='sample_name1')

        self.assertEqual(result['labmetadata_row_new_count'], 1)
        self.assertEqual(result['labmetadata_row_update_count'], 1)
        self.assertEqual(result['labmetadata_row_invalid_count'], 0)
        self.assertEqual(lab_meta.assay, new_assay)

    def test_labmetadata_row_duplicate(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataUnitTests.test_labmetadata_row_duplicate
        """
        row_duplicate = _generate_labmetadata_row_dict('3')

        mock_df = _generate_labmetadata_df([row_duplicate, row_duplicate])
        logger.info(f"\n{mock_df}")

        result = labmetadata_srv.persist_labmetadata(mock_df)

        logger.info("-" * 32)
        logger.info(json.dumps(result))

        self.assertEqual(result['labmetadata_row_update_count'], 0)  # the duplicate will get updated
        self.assertEqual(result['labmetadata_row_new_count'], 1)
        self.assertEqual(result['labmetadata_row_invalid_count'], 0)

    def test_labmetadata_non_nullable_columns(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataUnitTests.test_labmetadata_non_nullable_columns
        """
        row_1 = _generate_labmetadata_row_dict('1')
        row_2 = _generate_labmetadata_row_dict('2')
        row_3 = _generate_labmetadata_row_dict('3')
        row_4 = _generate_labmetadata_row_dict('4')
        row_5 = _generate_labmetadata_row_dict('5')
        row_6 = _generate_labmetadata_row_dict('6')

        row_1['sample_id'] = ''
        row_1['sample_name'] = ''
        row_1['library_id'] = ''
        row_3['sample_name'] = ''
        row_4['sample_id'] = ''
        row_5['library_id'] = ''

        mock_df = _generate_labmetadata_df([row_1, row_2, row_3, row_4, row_5, row_6, row_6])
        logger.info(f"\n{mock_df}")

        result = labmetadata_srv.persist_labmetadata(mock_df)

        logger.info("-" * 32)
        logger.info(json.dumps(result))

        self.assertEqual(result['labmetadata_row_update_count'], 0)
        self.assertEqual(result['labmetadata_row_new_count'], 2)  # row_2 and last 2 rows is duplicate
        self.assertEqual(result['labmetadata_row_invalid_count'], 4)  # 4 invalid
        self.assertEqual(LabMetadata.objects.count(), 2)

    def test_labmetadata_empty_columns(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataUnitTests.test_labmetadata_empty_columns
        """
        # set a bunch of nullable/empty-able rows 
        row_1 = _generate_labmetadata_row_dict('1')
        row_1['subject_id'] = '-'
        row_2 = _generate_labmetadata_row_dict('2')
        row_2['subject_id'] = ''
        row_3 = _generate_labmetadata_row_dict('3')
        row_3['coverage'] = np.nan
        row_4 = _generate_labmetadata_row_dict('4')
        row_4['subject_id'] = ' '

        mock_df = _generate_labmetadata_df([row_1, row_2, row_3, row_4])
        logger.info(f"\n{mock_df}")

        result = labmetadata_srv.persist_labmetadata(mock_df)

        logger.info("-" * 32)
        logger.info(json.dumps(result))

        self.assertEqual(LabMetadata.objects.count(), 4)
        self.assertEqual(result['labmetadata_row_new_count'], 4)


class LabMetadataIntegrationTests(LimsIntegrationTestCase):
    # some test case to hit actual API endpoint
    # annotate @skip to make the test cast to run through manual mean

    @skip
    def test_scheduled_update_handler(self):
        """
        python manage.py test data_processors.lims.lambdas.tests.test_labmetadata.LabMetadataIntegrationTests.test_scheduled_update_handler
        """
        result = labmetadata.scheduled_update_handler({'event': "LabMetadataIntegrationTests lims update event"}, None)

        logger.info("-" * 32)
        logger.info("Example LabMetadataIntegrationTests.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertGreater(result['labmetadata_row_new_count'], 1)

        logger.info(f"Total ingested rows into test db: {LabMetadata.objects.count()}")  # 1290 + 1174 = 2464
