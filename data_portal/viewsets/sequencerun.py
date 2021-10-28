# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.sequencerun import SequenceRun
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import SequenceRunSerializer

class SequenceRunViewSet(ReadOnlyModelViewSet):
    serializer_class = SequenceRunSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['id', 'run_id', 'date_modified', 'status', 'gds_folder_path', 'gds_volume_name', 'name']
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        run_id = self.request.query_params.get('run_id', None)
        name = self.request.query_params.get('name', None)
        run = self.request.query_params.get('run', None)
        instrument_run_id = self.request.query_params.get('instrument_run_id', None)
        status_ = self.request.query_params.get('status', None)
        return SequenceRun.objects.get_by_keyword(
            run_id=run_id,
            name=name,
            run=run,
            instrument_run_id=instrument_run_id,
            status=status_,
        )
