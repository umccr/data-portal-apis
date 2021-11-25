# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.labmetadata import LabMetadata
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import LabMetadataModelSerializer

logger = logging.getLogger()


class LabMetadataViewSet(ReadOnlyModelViewSet):
    serializer_class = LabMetadataModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-subject_id']
    search_fields = LabMetadata.get_base_fields()

    def get_queryset(self):
        return LabMetadata.objects.get_by_keyword(**self.request.query_params)
