import json
import logging
import urllib.parse
from datetime import timedelta
from typing import List

import pytz
from django.http import JsonResponse
from django.test import TestCase
from django.urls import reverse
from django.utils.timezone import now
from rest_framework import status
from rest_framework.test import APIClient

from data_portal.models import S3ObjectManager
from data_portal.tests.factories import S3ObjectFactory, LIMSRowFactory, S3LIMSFactory


class S3ObjectSearchResultRow:
    """
    Represents a single search result row.
    """
    def __init__(self, field_list: List) -> None:
        """
        The sequence of fields follows the S3ObjectSerializer
        :param field_list: list of field values
        """
        super().__init__()
        self.rn = field_list[0]
        self.bucket = field_list[1]
        self.key = field_list[2]
        self.path = field_list[3]
        self.size = field_list[4]
        self.last_modified_date = field_list[5]


def parse_s3_object_result_rows(response: JsonResponse):
    """
    Parse Http response to S3 object rows for the ease of testing
    :param response: response from making the request to the API
    :return: list of S3ObjectSearchResultRow objects
    """
    data = json.loads(response.content)
    rows = data['rows']['dataRows']
    result_list = []
    for row in rows:
        result = S3ObjectSearchResultRow(row)
        result_list.append(result)
    return result_list


class S3ObjectSearchTests(TestCase):
    """
    Test cases for S3 object searching/listing
    """
    def setUp(self) -> None:
        self.client = APIClient()

    def test_sorting_last_modified_date_valid(self):
        """
        Test the sorting behaviour for last_modified_date works as expected when query parameters are valid
        """
        s3_object_earlier = S3ObjectFactory(last_modified_date=now()-timedelta(days=2))
        s3_object_later = S3ObjectFactory(last_modified_date=now()-timedelta(days=1))

        # Sort by default (i.e. last_modified_date in descending order)
        response = self.client.get(reverse('file-search'))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        results = parse_s3_object_result_rows(response)
        self.assertEqual(results[0].rn, s3_object_later.id)

        # Sort by last_modified_date in ascending order
        response = self.client.get(reverse('file-search') + '?sortCol=last_modified_date&sortAsc=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        results = parse_s3_object_result_rows(response)
        self.assertEqual(results[0].rn, s3_object_earlier.id)

        # Sort by last_modified_date in descending order
        response = self.client.get(reverse('file-search') + '?sortCol=last_modified_date&sortAsc=false')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        results = parse_s3_object_result_rows(response)
        self.assertEqual(results[0].rn, s3_object_later.id)

    def test_sorting_size_valid(self):
        """
        Test the sorting behaviour for size works as expected when query parameters are valid
        """
        s3_object_smaller = S3ObjectFactory(size=1)
        s3_object_bigger = S3ObjectFactory(size=2)

        # Sort by size in ascending order
        response = self.client.get(reverse('file-search') + '?sortCol=size&sortAsc=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        results = parse_s3_object_result_rows(response)
        self.assertEqual(results[0].rn, s3_object_smaller.id)

        # Sort by size in descending order
        response = self.client.get(reverse('file-search') + '?sortCol=size&sortAsc=false')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        results = parse_s3_object_result_rows(response)
        self.assertEqual(results[0].rn, s3_object_bigger.id)

    def test_sorting_invalid(self):
        """
        Test the sorting behaviour works as expected when query parameters are invalid
        """
        # Try a non-sortable column
        response = self.client.get(reverse('file-search') + '?sortCol=rn&sortAsc=true')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_pagination_valid(self):
        """
        Test the pagination behaviour works as expected when query parameters are valid
        """
        s3_object_earlier = S3ObjectFactory(last_modified_date=now()-timedelta(days=1))
        s3_object_later = S3ObjectFactory(last_modified_date=now()-timedelta(days=2))

        # Paginate by one row per page
        response = self.client.get(reverse('file-search') + '?rowsPerPage=1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Check we do get one row only
        data = json.loads(response.content)
        results = parse_s3_object_result_rows(response)
        self.assertEqual(len(results), 1)

        # Check meta data
        meta = data['meta']
        self.assertEqual(meta['size'], 1)
        self.assertEqual(meta['page'], 1)
        self.assertEqual(meta['start'], 1)
        self.assertEqual(meta['totalRows'], 2)
        self.assertEqual(meta['totalPages'], 2)

    def test_pagination_invalid(self):
        """
        Test the pagination behaviour works as expected when query parameters are invalid
        """
        s3_object_earlier = S3ObjectFactory(last_modified_date=now()-timedelta(days=1))
        s3_object_later = S3ObjectFactory(last_modified_date=now()-timedelta(days=2))

        # Use an invalid page number
        response = self.client.get(reverse('file-search') + '?rowsPerPage=1&page=-1')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Use an invalid rows per page number
        response = self.client.get(reverse('file-search') + '?rowsPerPage=aa&page=1')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_random_samples_valid(self):
        """
        Test random sampling works with valid query parameters.
        """
        s3_object_earlier = S3ObjectFactory(last_modified_date=now() - timedelta(days=1))
        s3_object_later = S3ObjectFactory(last_modified_date=now() - timedelta(days=2))

        # Get one random sample
        response = self.client.get(reverse('file-search') + '?randomSamples=1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        results = parse_s3_object_result_rows(response)
        self.assertEqual(len(results), 1)

    def test_random_samples_invalid(self):
        """
        Test random sampling returns error with invalid query parameters.
        """
        s3_object_earlier = S3ObjectFactory(last_modified_date=now() - timedelta(days=1))
        s3_object_later = S3ObjectFactory(last_modified_date=now() - timedelta(days=2))

        # Use a number larger than the maximum limit
        response = self.client.get(
            reverse('file-search') + '?randomSamples=%d' % (S3ObjectManager.MAX_RAND_SAMPLES_LIMIT + 1)
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_search_query_valid(self):
        """
        Test search query (parser) works expected for all supported valid query types
        * default
        * pathinc
        * ext
        * date: all comparison operators
        * size: all comparison operators
        * subject_id: filled and not filled
        * sample_id: filled and not filled
        * linked: true and false
        * case: enabled and not enabled
        """
        file_name_without_ext = 's3_object_earlier'
        ext = 'csv'
        size = 1
        date = now().astimezone(pytz.timezone('UTC')).date()
        date_format = '%Y-%m-%d'

        s3_object = S3ObjectFactory(
            key='%s.%s' % (file_name_without_ext.upper(), ext.upper()),  # Use upper case to test case sensitivity
            last_modified_date=date,
            size=size
        )

        # Not linked with any LIMS row
        not_linked_s3_object = S3ObjectFactory()

        subject_id = "subject_id"
        external_subject_id = "external_subject_id"
        sample_id = "sample_id"
        lims_row = LIMSRowFactory(subject_id=subject_id, external_subject_id=external_subject_id, sample_id=sample_id)
        s3_lims = S3LIMSFactory(s3_object=s3_object, lims_row=lims_row)

        # Prepare a valid query that covers all supported features
        default_query = '%s ' % file_name_without_ext  # Append some space around also to test for string stripping
        ext_query = 'ext:%s' % ext
        pathinc_query = 'pathinc:%s' % file_name_without_ext

        # Try all valid comparators and use edge values
        size_query = 'size:=%d size:>=%d size:<=%d size:>%d size:<%d' % (size, size, size, size-1, size+1)
        last_modified_date_query = 'date:=%s date:>=%s date:<=%s date:>%s date:<%s' % (
            date.strftime(date_format),
            date.strftime(date_format),
            date.strftime(date_format),
            (date-timedelta(days=1)).strftime(date_format),
            (date+timedelta(days=1)).strftime(date_format),
        )

        # Test both subject_id and external_subject_id
        lims_join_query_base = 'subjectid:%s subjectid:%s sampleid:%s' % (subject_id, external_subject_id, sample_id)
        case_query = 'case:true pathinc:%s ext:%s' % (file_name_without_ext.upper(), ext.upper())

        # All testable queries
        # query_no_illumina_id is the base query
        query_no_lims_join = [default_query, ext_query, pathinc_query, size_query, last_modified_date_query]
        query_with_lims_join = query_no_lims_join.copy() + [lims_join_query_base]
        query_linked_filter_true = ['linked:true']
        query_linked_filter_false = ['linked:false']
        query_default_case_sensitivity = query_no_lims_join.copy()
        query_case_sensitive = [size_query, last_modified_date_query, case_query]

        # List of all testable queries + the expected s3 object to be returned in the result
        query_list = [
            (query_no_lims_join, s3_object),
            (query_with_lims_join, s3_object),
            (query_linked_filter_true, s3_object),
            (query_linked_filter_false, not_linked_s3_object),
            (query_default_case_sensitivity, s3_object),
            (query_case_sensitive, s3_object)
        ]

        logger = logging.getLogger()

        # Test one by one
        for sub_query_list, expected_s3_object in query_list:
            query_string = ' '.join(sub_query_list)
            query_string_encoded = urllib.parse.quote(query_string.encode('utf8'))
            response = self.client.get(reverse('file-search') + '?query=%s' % query_string_encoded)

            self.assertEqual(response.status_code, status.HTTP_200_OK, msg=str(json.loads(response.content)))
            results = parse_s3_object_result_rows(response)
            # We should always get the one matching s3 object
            self.assertEqual(len(results), 1, msg='Query: %s' % query_string)
            self.assertEqual(results[0].rn, expected_s3_object.id, msg='Query: %s' % query_string)

    def test_search_query_invalid(self):
        """
        Test search query (parser) returns error for invalid query
        * size: valid comparator but invalid value
        * size: valid value but no comparator
        * date: valid comparator but invalid value
        * date: valid value but no comparator
        * invalid filter type
        """

        invalid_query = 'size:=a size:1 date:=2019-01-x date:2019-01-01 haha:a'
        query_string_encoded = urllib.parse.quote(invalid_query.encode('utf8'))
        response = self.client.get(reverse('file-search') + '?query=%s' % query_string_encoded)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
