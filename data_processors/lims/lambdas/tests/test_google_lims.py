import json
import tempfile
from io import BytesIO
from typing import List, Dict, Union
from unittest import skip

import pandas as pd
from libumccr import libgdrive
from libumccr.aws import libssm
from mockito import when

from data_portal.models.limsrow import LIMSRow
from data_processors import const
from data_processors.lims.lambdas import google_lims
from data_processors.lims.services import google_lims_srv
from data_processors.lims.tests.case import LimsUnitTestCase, LimsIntegrationTestCase, logger

# columns in LIMS CSV
lims_csv_columns = [
    'IlluminaID', 'Run', 'Timestamp', 'SubjectID', 'SampleID', 'LibraryID',
    'ExternalSubjectID', 'ExternalSampleID', 'ExternalLibraryID', 'SampleName',
    'ProjectOwner', 'ProjectName', 'Type', 'Assay', 'Phenotype', 'Source',
    'Quality', 'Topup', 'SecondaryAnalysis', 'FASTQ', 'NumberFASTQS', 'Results', 'Trello', 'Notes', 'ToDo'
]

_mock_lims_sheet_content = b"""
IlluminaID,Run,Timestamp,SubjectID,SampleID,LibraryID,ExternalSubjectID,ExternalSampleID,ExternalLibraryID,SampleName,ProjectOwner,ProjectName,ProjectCustodian,Type,Assay,OverrideCycles,Phenotype,Source,Quality,Topup,SecondaryAnalysis,Workflow,Tags,FASTQ,NumberFASTQS,Results,Trello,Notes,ToDo
210331_A01052_0041_ABCDEFGHIJ,41,2021-03-31,SBJ00001,PRJ000001,L0000001,EXTS001,EXT099999-DNAD001-T,-,PRJ000001-EXT099999-DNAD001-T,UMCCR,CUP,John Doe <jd@me.au>,WGS,TsqNano,Y151;I8;I8;Y151,tumor,FFPE,borderline,-,wgs_40,clinical,-,s3://fastq-bucket/Folder/Project/Sample/L2100220*.fastq.gz,2,s3://primary-bucket/project/SBJ00001/WGS/2021-04-08/   ,-,-,FALSE
"""


def _generate_lims_csv_row_dict(id: str) -> dict:
    """
    Generate LIMS csv row dict
    :param id: id of the row, to make this row distinguishable
    :return: row dict
    """
    row = dict()
    for col in lims_csv_columns:
        if col == 'Run':
            row[col] = '1'
        elif col == 'Timestamp':
            row[col] = '2019-01-01'
        else:
            # Normal columns, just use column name as value + id
            row[col] = col + id
    return row


def _generate_lims_csv(rows: List[Dict[str, str]]):
    csv_data = ','.join(lims_csv_columns) + '\n'  # Generate header row

    for row in rows:
        csv_data += ','.join(row.values()) + '\n'

    return csv_data


def _df(b: Union[bytes, BytesIO]):
    return pd.read_csv(b, dtype=str)


class LimsUnitTests(LimsUnitTestCase):

    def setUp(self) -> None:
        super(LimsUnitTests, self).setUp()

    def tearDown(self) -> None:
        super(LimsUnitTests, self).tearDown()  # parent tear down should call last

    def test_scheduled_update_handler(self):
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsUnitTests.test_scheduled_update_handler
        """

        mock_lims_sheet = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)  # delete=False keep file in tmp dir
        mock_lims_sheet.write(_mock_lims_sheet_content.lstrip().rstrip())
        mock_lims_sheet.seek(0)
        mock_lims_sheet.flush()

        # print csv file in tmp dir -- if delete=False, you can see the mock csv content
        logger.info(f"Path to mock lims sheet: {mock_lims_sheet.name}")

        when(google_lims.libgdrive).download_sheet(...).thenReturn(_df(mock_lims_sheet))

        result = google_lims.scheduled_update_handler({'event': "mock lims update event"}, None)

        logger.info("-" * 32)
        logger.info("Example google_lims.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertEqual(result['lims_row_new_count'], 1)

        sbj = LIMSRow.objects.get(subject_id='SBJ00001')
        logger.info(sbj)
        self.assertIsNotNone(sbj)

        # clean up
        mock_lims_sheet.close()

    def test_lims_rewrite(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsUnitTests.test_lims_rewrite
        """
        subject_id = 'subject_id'
        sample_id = 'sample_id'

        row_1 = _generate_lims_csv_row_dict('1')
        row_1['SampleID'] = sample_id
        row_1['SubjectID'] = subject_id

        process_results = google_lims_srv.persist_lims_data(_df(BytesIO(_generate_lims_csv([row_1]).encode())), True)

        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(LIMSRow.objects.get(illumina_id='IlluminaID1', sample_id=sample_id).results, row_1['Results'])
        self.assertEqual(process_results['lims_row_update_count'], 0)

    def test_lims_update(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsUnitTests.test_lims_update
        """
        row_1 = _generate_lims_csv_row_dict('1')
        google_lims_srv.persist_lims_data(_df(BytesIO(_generate_lims_csv([row_1]).encode())))

        new_results = 'NewResults'
        row_1['Results'] = new_results
        row_2 = _generate_lims_csv_row_dict('2')
        process_results = google_lims_srv.persist_lims_data(_df(BytesIO(_generate_lims_csv([row_1, row_2]).encode())))

        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(process_results['lims_row_update_count'], 1)
        self.assertEqual(LIMSRow.objects.get(illumina_id='IlluminaID1', sample_id='SampleID1').results, new_results)

    def test_lims_row_duplicate(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsUnitTests.test_lims_row_duplicate
        """
        row_duplicate = _generate_lims_csv_row_dict('3')
        process_results = google_lims_srv.persist_lims_data(_df(BytesIO(
            _generate_lims_csv([row_duplicate, row_duplicate]).encode()
        )))
        self.assertEqual(process_results['lims_row_invalid_count'], 1)

    def test_lims_non_nullable_columns(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsUnitTests.test_lims_non_nullable_columns

        Test to process non-nullable columns and rollback if one row doesn't have the required values
        """
        row_1 = _generate_lims_csv_row_dict('1')
        row_2 = _generate_lims_csv_row_dict('2')

        # Use blank values for all non-nullable fields
        row_1['IlluminaID'] = '-'
        row_1['Run'] = '-'
        row_1['Timestamp'] = '-'
        row_1['SampleID'] = '-'
        row_1['LibraryID'] = '-'

        process_results = google_lims_srv.persist_lims_data(_df(BytesIO(_generate_lims_csv([row_1, row_2]).encode())))

        self.assertEqual(LIMSRow.objects.count(), 1)
        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(process_results['lims_row_invalid_count'], 1)

    def test_lims_empty_subject_id(self) -> None:
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsUnitTests.test_lims_empty_subject_id

        Test LIMS row with empty SubjectID
        """
        row_1 = _generate_lims_csv_row_dict('1')

        row_1['SubjectID'] = '-'
        process_results = google_lims_srv.persist_lims_data(_df(BytesIO(_generate_lims_csv([row_1]).encode())))

        self.assertEqual(LIMSRow.objects.count(), 1)
        self.assertEqual(process_results['lims_row_new_count'], 1)


class LimsIntegrationTests(LimsIntegrationTestCase):
    # some test case to hit actual API endpoint
    # annotate @skip to make the test cast to run through manual mean

    @skip
    def test_scheduled_update_handler(self):
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsIntegrationTests.test_scheduled_update_handler
        """
        result = google_lims.scheduled_update_handler({'event': "LimsIntegrationTests lims update event"}, None)

        logger.info("-" * 32)
        logger.info("Example google_lims.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertGreater(result['lims_row_new_count'], 1)

        logger.info(f"Total ingested rows into test db: {LIMSRow.objects.count()}")

    @skip
    def test_scheduled_update_handler_crlf(self):
        """
        python manage.py test data_processors.lims.lambdas.tests.test_google_lims.LimsIntegrationTests.test_scheduled_update_handler_crlf

        See https://github.com/umccr/data-portal-apis/issues/395
        """

        lims_sheet_id = "1EgqIxmoJxjmxuaBJP0NpRVJgUtPesON6knVXqQDSUMA"  # Google Sheet with a cell having Windows CRLF
        account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

        # bytes_data = libgdrive.download_sheet1_csv(account_info, lims_sheet_id)
        # with open("lims_mock.csv", "wb") as out:
        #     out.write(bytes_data)
        # result = google_lims_srv.persist_lims_data(BytesIO(bytes_data))

        df: pd.DataFrame = libgdrive.download_sheet(account_info, lims_sheet_id, "Sheet1")
        # df.to_csv("lims_mock2.csv", index=False)
        # result = google_lims_srv.persist_lims_data(BytesIO(df.to_csv().encode()))
        result = google_lims_srv.persist_lims_data(df)

        logger.info("-" * 32)
        logger.info("Example google_lims.scheduled_update_handler lambda output:")
        logger.info(json.dumps(result))
        self.assertEqual(result['lims_row_new_count'], 3)

        logger.info(f"Total ingested rows into test db: {LIMSRow.objects.count()}")

        lib_79 = LIMSRow.objects.get(library_id='L2200079')
        logger.info(lib_79)
        logger.info(lib_79.external_subject_id)
        logger.info(lib_79.project_owner)
        self.assertIsNotNone(lib_79.external_subject_id)
        self.assertIsNotNone(lib_79.project_owner)
