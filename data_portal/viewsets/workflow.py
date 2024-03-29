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
    ordering_fields = '__all__'
    ordering = ['-id']
    search_fields = Workflow.get_base_fields()

    def get_queryset(self):
        return Workflow.objects.get_by_keyword(**self.request.query_params)
