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

# TODO to be refactored
SEQUENCE_SEARCH_FIELDS = ["id", "instrument_run_id", "run_id", "sample_sheet_name", "gds_folder_path",
                          "gds_volume_name", "reagent_barcode", "flowcell_barcode", "status", "start_time", "end_time"]

class SequenceViewSet(ReadOnlyModelViewSet):
    serializer_class = SequenceSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-id']
    search_fields = SEQUENCE_SEARCH_FIELDS

    def get_queryset(self):
        return Sequence.objects.get_by_keyword(
            keywords=self.request.query_params
        )
