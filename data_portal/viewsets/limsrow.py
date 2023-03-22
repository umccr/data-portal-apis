# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.limsrow import LIMSRow
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import LIMSRowModelSerializer

logger = logging.getLogger()

allowed_fields = ['project_name', 'project_owner', 'workflow', 'source', 'assay', 'type', 'phenotype']


class LIMSRowViewSet(ReadOnlyModelViewSet):
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-subject_id']
    search_fields = LIMSRow.get_base_fields()

    def get_queryset(self):
        return LIMSRow.objects.get_by_keyword(**self.request.query_params)

    @action(detail=False, methods=['get'])
    def by_aggregate_count(self, request):
        fields = self.request.query_params.getlist('fields', None)

        if fields is None or not fields:
            return Response(data={})

        if 'all' in fields:
            fields.clear()
            fields.extend(allowed_fields)

        fields = list(set(fields) & set(allowed_fields))  # intersect

        data = {}
        for f in fields:
            data.update({
                str(f): LIMSRow.objects.get_by_aggregate_count(str(f))
            })

        return Response(data=data)

    @action(detail=False, methods=['get'])
    def by_cube(self, request):
        fields = self.request.query_params.getlist('fields', None)

        if fields is None or not fields or len(fields) < 2:
            return Response(data={})

        if not all(item in allowed_fields for item in fields):
            return Response(data={})

        data = LIMSRow.objects.get_by_cube(field_left=fields[0], field_right=fields[1], field_sort=fields[0])

        return Response(data=data)
