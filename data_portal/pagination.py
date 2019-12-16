from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.settings import api_settings


class StandardResultsSetPagination(PageNumberPagination):
    page_size = api_settings.PAGE_SIZE
    page_size_query_param = 'rowsPerPage'
    max_page_size = 1000

    def get_paginated_response(self, data):
        return Response({
            'links': {
                'next': self.get_next_link(),
                'previous': self.get_previous_link(),
            },
            'pagination': {
                'count': self.page.paginator.count,
                'page': self.page.number,
                'rowsPerPage': self.get_page_size(self.request),
            },
            'results': data
        })
