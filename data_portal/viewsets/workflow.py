# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet


from data_portal.models.workflow import Workflow
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import WorkflowSerializer

class WorkflowViewSet(ReadOnlyModelViewSet):
    serializer_class = WorkflowSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['id', 'wfr_name', 'sample_name', 'type_name', 'wfr_id', 'version', 'start', 'end', 'end_status']
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        # Workflow model keyword
        sequence_run = self.request.query_params.get('sequence_run', None)
        sequence = self.request.query_params.get('sequence', None)
        run = self.request.query_params.get('run', None)
        sample_name = self.request.query_params.get('sample_name', None)
        type_name = self.request.query_params.get('type_name', None)
        end_status = self.request.query_params.get('end_status', None)

        # Libraryrun model keyword
        library_id = self.request.query_params.get('library_id', None)

        return Workflow.objects.get_by_keyword(
            sequence_run=sequence_run,
            sequence=sequence,
            run=run,
            sample_name=sample_name,
            type_name=type_name,
            end_status=end_status,
            library_id=library_id,
        )
