# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.sequence import Sequence
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import SequenceSerializer


class SequenceViewSet(ReadOnlyModelViewSet):
    serializer_class = SequenceSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-id']
    search_fields = Sequence.get_base_fields()

    def get_queryset(self):
        return Sequence.objects.get_by_keyword(**self.request.query_params)
