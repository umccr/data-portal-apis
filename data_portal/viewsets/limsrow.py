# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.limsrow import LIMSRow
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import LIMSRowModelSerializer

logger = logging.getLogger()


class LIMSRowViewSet(ReadOnlyModelViewSet):
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-subject_id']
    search_fields = LIMSRow.get_base_fields()

    def get_queryset(self):
        return LIMSRow.objects.get_by_keyword(**self.request.query_params)
