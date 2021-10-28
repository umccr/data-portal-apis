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


# TODO to be refactored
LIMS_SEARCH_ORDER_FIELDS = [
    'subject_id', 'timestamp', 'type', 'run', 'sample_id', 'external_subject_id', 'results', 'phenotype',
    'library_id', 'external_sample_id', 'project_name', 'illumina_id',
]

class LIMSRowViewSet(ReadOnlyModelViewSet):
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = LIMS_SEARCH_ORDER_FIELDS
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        subject = self.request.query_params.get('subject', None)
        run = self.request.query_params.get('run', None)
        return LIMSRow.objects.get_by_keyword(subject=subject, run=run)
