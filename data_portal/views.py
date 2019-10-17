from typing import Tuple, Optional

import boto3
from botocore.exceptions import ClientError
from django.core.exceptions import EmptyResultSet
from django.core.paginator import Paginator, EmptyPage
from django.http import JsonResponse
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
import logging

from data_portal.exceptions import InvalidSearchQuery, InvalidQueryParameter
from data_portal.models import S3Object, S3ObjectManager
from data_portal.responses import JsonErrorResponse
from data_portal.s3_object_search import S3ObjectSearchQueryHelper
from data_portal.serializers import S3ObjectSerializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_ROWS_PER_PAGE = 20


def parse_sorting_params(query_params: dict) -> Tuple[str, str]:
    """
    Parse query parameters for sorting
    """
    sort_col = query_params.get('sortCol', S3Object.DEFAULT_SORT_COL)
    sort_asc_raw = query_params.get('sortAsc', None)
    is_sort_asc = sort_asc_raw is not None and sort_asc_raw.lower() == 'true'
    sort_asc_prefix = '-' if not is_sort_asc else ''

    # Ignore null value for sort col
    if sort_col == "null":
        sort_col = S3Object.DEFAULT_SORT_COL

    if sort_col not in S3Object.SORTABLE_COLUMNS:
        raise InvalidQueryParameter('sortCol', sort_col, 'The column is either unknown or not allowed to be sorted.')

    return sort_col, sort_asc_prefix


def parse_pagination_params(query_params: dict) -> Tuple[int, int]:
    """
    Parse query parameters for pagination
    """
    rows_per_page = str(query_params.get('rowsPerPage', DEFAULT_ROWS_PER_PAGE))
    page = query_params.get('page', '0')

    if not rows_per_page.isdigit():
        raise InvalidQueryParameter('rowsPerPage', rows_per_page, 'rowsPerPage must be a non-negative integer.')
    if not page.isdigit():
        raise InvalidQueryParameter('page', page, 'page must be a non-negative integer.')

    return int(rows_per_page), int(page)


def parse_random_samples(query_params: dict) -> Optional[int]:
    """
    Parse query parameters for random sampling
    """
    random_samples_raw = query_params.get('randomSamples', None)

    if random_samples_raw is not None and not random_samples_raw.isdigit():
        raise InvalidQueryParameter('randomSamples', '', 'randomSamples must be a non-negative integer.')

    if random_samples_raw is not None and int(random_samples_raw) > S3ObjectManager.MAX_RAND_SAMPLES_LIMIT:
        raise InvalidQueryParameter('randomSamples', '', 'randomSamples is too large.')

    return int(random_samples_raw) if random_samples_raw is not None else None


@api_view(['GET'])
def search_file(request: Request):
    """
    query_params
    * query:          str,    mandatory,  the query string (leave empty for no filter)
    * rowsPerPage:    int,    optional,   number of rows per page. (no effect is randomSamples is set).
    * page:           int,    optional,   the current page number. 0-based.
    * sortCol:        str,    optional,   the column to be sorted
    * sortAsc:        bool,   optional,   sort in ascending order
    * randomSamples:  int,    optional,   retrieve n randomly-selected samples
    """
    query_params = request.query_params
    query = query_params.get('query', '')

    try:
        rows_per_page, page = parse_pagination_params(query_params)
        sort_col, sort_asc_prefix = parse_sorting_params(query_params)
        random_samples = parse_random_samples(query_params)
    except InvalidQueryParameter as e:
        return JsonErrorResponse('invalid query parameters: ' + str(e), status=status.HTTP_400_BAD_REQUEST)

    # Apply random sampling if required
    if random_samples is not None:
        query_set = S3Object.objects.random_samples(random_samples)
        # For random sampling, default rows per page doesn't apply
        rows_per_page = S3ObjectManager.MAX_RAND_SAMPLES_LIMIT
    else:
        query_set = S3Object.objects

    # Parse and apply filters
    try:
        query_set = S3ObjectSearchQueryHelper.parse(query, query_set)
    except InvalidSearchQuery as e:
        return JsonErrorResponse(str(e), status=status.HTTP_400_BAD_REQUEST)

    # Apply ordering
    query_set = query_set.order_by(sort_asc_prefix + sort_col)

    try:
        logger.info('Query to be executed: (without pagination) %s ' % query_set.query)
    except EmptyResultSet as e:
        logger.info("No data available")
        return JsonResponse(data={}, status=status.HTTP_200_OK)

    # Apply pagination
    paginator = Paginator(query_set, per_page=rows_per_page)
    object_page = paginator.get_page(page + 1)
    object_list = object_page.object_list
    empty_record = len(object_list) == 0

    serializer = S3ObjectSerializer(object_list, many=True)

    # Compose meta information
    meta_data = {
        'size': rows_per_page,
        'page': page + 1,
        'start': object_page.start_index(),
        'totalRows': paginator.count,
        'totalPages': paginator.num_pages
    }

    data = {
        'meta': meta_data,
        'rows': {
            'headerRow': [],
            'dataRows': serializer.data
        }
    }

    # Retrieve the header row if we have at least one record
    if not empty_record:
        single_serializer = S3ObjectSerializer(object_list[0])
        data['rows']['headerRow'] = single_serializer.get_fields_with_sortable()

    return JsonResponse(data=data, status=status.HTTP_200_OK)


@api_view(['GET'])
def sign_s3_file(request: Request):
    """
    query params
    * bucket: str,    mandatory,  the bucket name
    * key:    str,    mandatory,  the s3 object key
    """
    query_params = request.query_params
    bucket = query_params.get('bucket', None)
    key = query_params.get('key', None)

    if bucket is None or key is None:
        return JsonErrorResponse('Missing required parameters: bucket / key', status=status.HTTP_400_BAD_REQUEST)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object', Params={
            'Bucket': bucket,
            'Key': key
        })
    except ClientError as e:
        logging.error(e)
        return JsonErrorResponse('Failed to sign the specified s3 object', status=status.HTTP_400_BAD_REQUEST)

    return JsonResponse(data=response, status=status.HTTP_200_OK, safe=False)
