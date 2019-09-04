from django.http import JsonResponse
from rest_framework.decorators import api_view
from rest_framework.request import Request


# /**
#  * event.queryStringParameters:
#  * {
#  *      query:          string, mandatory,  the query string (leave empty for no filter)
#  *      rowsPerPage:    int,    optional,   number of rows per page
#  *      page:           int,    opti onal,   the current page number
#  *      sortCol:        string, optional,   the column to be sorted
#  *      sortAsc:        bool,   optional,   sort in ascending order
#  *      randomSamples:  int,    optional,   retrieve n randomly-selected samples
#  * }
#  */


@api_view(['GET'])
def search_file(request: Request):
    query = request.query_params.get('query', '')
    rows_per_page = request.query_params.get('rowsPerPage', 20)
    page = request.query_params.get('page', 0)
    sort_col = request.query_params.get('sortCol', None)
    sort_asc = request.query_params.get('sortAsc', None)
    random_samples = request.query_params.get('randomSamples', None)

    return JsonResponse(data={})


@api_view(['GET'])
def sign_s3_file(request: Request):
    return JsonResponse(data={})
