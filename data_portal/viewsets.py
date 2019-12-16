import logging

from rest_framework import viewsets

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
    queryset = LIMSRow.objects.order_by('-timestamp', '-id').all()
    logger.debug('Query to be executed: %s ' % queryset.query)
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
