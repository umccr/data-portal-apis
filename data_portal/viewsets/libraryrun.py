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
        # From Library model
        library_id = self.request.query_params.get('library_id', None)
        instrument_run_id = self.request.query_params.get('instrument_run_id', None)
        run_id = self.request.query_params.get('run_id', None)
        lane = self.request.query_params.get('lane', None)
        override_cycles = self.request.query_params.get('override_cycles', None)
        coverage_yield = self.request.query_params.get('coverage_yield', None)
        qc_pass = self.request.query_params.get('qc_pass', None)
        qc_status = self.request.query_params.get('qc_status', None)
        valid_for_analysis = self.request.query_params.get('valid_for_analysis', None)

        # From Workflow model
        type_name = self.request.query_params.get('type_name', None)
        end_status = self.request.query_params.get('end_status', None)

        return LibraryRun.objects.get_by_keyword(
            library_id=library_id,
            instrument_run_id=instrument_run_id,
            run_id=run_id,
            lane=lane,
            override_cycles=override_cycles,
            coverage_yield=coverage_yield,
            qc_pass=qc_pass,
            qc_status=qc_status,
            valid_for_analysis=valid_for_analysis,
            type_name=type_name,
            end_status=end_status,
        )
