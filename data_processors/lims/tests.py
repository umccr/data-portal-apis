import logging
from io import BytesIO
from typing import List, Dict

from django.test import TestCase

from data_portal.models import LIMSRow
from data_processors.lims.services import persist_lims_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# All columns in a LIMS CSV
lims_csv_columns = [
    'IlluminaID', 'Run', 'Timestamp', 'SubjectID', 'SampleID', 'LibraryID',
    'ExternalSubjectID', 'ExternalSampleID', 'ExternalLibraryID', 'SampleName',
    'ProjectOwner', 'ProjectName', 'Type', 'Assay', 'Phenotype', 'Source',
    'Quality', 'Topup', 'SecondaryAnalysis', 'FASTQ', 'NumberFASTQS', 'Results', 'Trello', 'Notes', 'ToDo'
]


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


class LIMSTests(TestCase):

    def test_lims_rewrite(self) -> None:
        subject_id = 'subject_id'
        sample_id = 'sample_id'

        row_1 = _generate_lims_csv_row_dict('1')
        row_1['SampleID'] = sample_id
        row_1['SubjectID'] = subject_id

        process_results = persist_lims_data(BytesIO(_generate_lims_csv([row_1]).encode()), True)

        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(LIMSRow.objects.get(illumina_id='IlluminaID1', sample_id=sample_id).results, row_1['Results'])
        self.assertEqual(process_results['lims_row_update_count'], 0)

    def test_lims_update(self) -> None:
        row_1 = _generate_lims_csv_row_dict('1')
        persist_lims_data(BytesIO(_generate_lims_csv([row_1]).encode()))

        new_results = 'NewResults'
        row_1['Results'] = new_results
        row_2 = _generate_lims_csv_row_dict('2')
        process_results = persist_lims_data(BytesIO(_generate_lims_csv([row_1, row_2]).encode()))

        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(process_results['lims_row_update_count'], 1)
        self.assertEqual(LIMSRow.objects.get(illumina_id='IlluminaID1', sample_id='SampleID1').results, new_results)

    def test_lims_row_duplicate(self) -> None:
        row_duplicate = _generate_lims_csv_row_dict('3')
        process_results = persist_lims_data(BytesIO(_generate_lims_csv([row_duplicate, row_duplicate]).encode()))
        self.assertEqual(process_results['lims_row_invalid_count'], 1)

    def test_lims_non_nullable_columns(self) -> None:
        """
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

        process_results = persist_lims_data(BytesIO(_generate_lims_csv([row_1, row_2]).encode()))

        self.assertEqual(LIMSRow.objects.count(), 1)
        self.assertEqual(process_results['lims_row_new_count'], 1)
        self.assertEqual(process_results['lims_row_invalid_count'], 1)

    def test_lims_empty_subject_id(self) -> None:
        """
        Test LIMS row with empty SubjectID
        """
        row_1 = _generate_lims_csv_row_dict('1')

        row_1['SubjectID'] = '-'
        process_results = persist_lims_data(BytesIO(_generate_lims_csv([row_1]).encode()))

        self.assertEqual(LIMSRow.objects.count(), 1)
        self.assertEqual(process_results['lims_row_new_count'], 1)
