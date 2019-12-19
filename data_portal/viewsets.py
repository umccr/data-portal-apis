import logging

from rest_framework import viewsets, filters

from .models import LIMSRow
from .pagination import StandardResultsSetPagination
from .serializers import LIMSRowModelSerializer

logger = logging.getLogger()


class ReadOnlyListViewset(
    viewsets.mixins.RetrieveModelMixin,
    viewsets.mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    pass


class LIMSRowViewSet(ReadOnlyListViewset):
    queryset = LIMSRow.objects.all()
    logger.debug('Query to be executed: %s ' % queryset.query)
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = [
        'subject_id', 'timestamp', 'type', 'run', 'sample_id', 'external_subject_id', 'results', 'phenotype',
        'library_id', 'external_sample_id', 'project_name'
    ]
    ordering = ['-subject_id']
    search_fields = ordering_fields
