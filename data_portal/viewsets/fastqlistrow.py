# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import FastqListRowSerializer

logger = logging.getLogger()


class FastqListRowViewSet(ReadOnlyModelViewSet):
    serializer_class = FastqListRowSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['id', 'rgid', 'rgsm', 'rglb', 'lane']
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        run = self.request.query_params.get('run', None)
        rgid = self.request.query_params.get('rgid', None)
        rgsm = self.request.query_params.get('rgsm', None)
        rglb = self.request.query_params.get('rglb', None)
        lane = self.request.query_params.get('lane', None)
        project_owner = self.request.query_params.get('project_owner', None)

        return FastqListRow.objects.get_by_keyword(
            run=run,
            rgid=rgid,
            rgsm=rgsm,
            rglb=rglb,
            lane=lane,
            project_owner=project_owner
        )
