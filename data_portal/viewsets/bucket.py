# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import filters
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.s3object import S3Object
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import BucketIdSerializer

logger = logging.getLogger()


class BucketViewSet(ReadOnlyModelViewSet):
    queryset = S3Object.objects.values_list('bucket', named=True).order_by('bucket').distinct()
    serializer_class = BucketIdSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['bucket']
    ordering = ['-bucket']
    search_fields = ordering_fields
