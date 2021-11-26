# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import FastqListRowSerializer

logger = logging.getLogger()


class FastqListRowViewSet(ReadOnlyModelViewSet):
    serializer_class = FastqListRowSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['id', 'rgid', 'rgsm', 'rglb', 'lane']
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        return FastqListRow.objects.get_by_keyword(**self.request.query_params)
