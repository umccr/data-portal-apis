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

# TODO to be refactored
METADATA_SEARCH_ORDER_FIELDS = [
    'library_id', 'sample_name', 'sample_id', 'external_sample_id', 'subject_id', 'external_subject_id', 'phenotype',
    'quality', 'source', 'project_name', 'project_owner', 'experiment_id', 'type', 'assay', 'workflow',
]


class LabMetadataViewSet(ReadOnlyModelViewSet):
    serializer_class = LabMetadataModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = METADATA_SEARCH_ORDER_FIELDS
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        subject = self.request.query_params.get('subject', None)
        sample = self.request.query_params.get('sample', None)
        library = self.request.query_params.get('library', None)
        phenotype = self.request.query_params.get('phenotype', None)
        type_ = self.request.query_params.get('type', None)
        project_owner = self.request.query_params.get('project_owner', None)
        project_name = self.request.query_params.get('project_name', None)

        return LabMetadata.objects.get_by_keyword(
            subject_id=subject,
            sample_id=sample,
            library_id=library,
            phenotype=phenotype,
            type=type_,
            project_owner=project_owner,
            project_name=project_name,
        )
