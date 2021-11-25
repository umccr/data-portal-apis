# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.report import Report
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import ReportSerializer

logger = logging.getLogger()


class ReportViewSet(ReadOnlyModelViewSet):
    serializer_class = ReportSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['subject_id', 'sample_id', 'library_id', 'type']
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        return Report.objects.get_by_keyword(**self.request.query_params)
