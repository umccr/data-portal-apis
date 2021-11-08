# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.libraryrun import LibraryRun
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import LibraryRunModelSerializer

logger = logging.getLogger()

# TODO to be refactored
LIBRARY_RUN_SEARCH_FIELDS = ["id", "library_id", "instrument_run_id", "run_id", "lane", "override_cycles",
                             "coverage_yield", "qc_pass", "qc_status", "valid_for_analysis"]


class LibraryRunViewSet(ReadOnlyModelViewSet):
    serializer_class = LibraryRunModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-id']
    search_fields = LIBRARY_RUN_SEARCH_FIELDS

    def get_queryset(self):
        return LibraryRun.objects.get_by_keyword(
            keywords=self.request.query_params
        )
