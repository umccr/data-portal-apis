# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from django.db import InternalError
from libica.app import gds
from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.gdsfile import GDSFile
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import GDSFileModelSerializer

logger = logging.getLogger()


class GDSFileViewSet(ReadOnlyModelViewSet):
    serializer_class = GDSFileModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['path']
    search_fields = ['$path']

    def get_queryset(self):
        volume_name = self.request.query_params.get('volume_name', None)
        subject = self.request.query_params.get('subject', None)
        run = self.request.query_params.get('run', None)
        return GDSFile.objects.get_by_keyword(volume_name=volume_name, subject=subject, run=run)

    @action(detail=True)
    def presign(self, request, pk=None):
        obj: GDSFile = self.get_object()

        presigned_url_mode = request.headers.get('Content-Disposition')
        response = gds.presign_gds_file(file_id=obj.file_id, volume_name=obj.volume_name, path_=obj.path,
                                        presigned_url_mode=presigned_url_mode)

        if response[0]:
            return Response({'signed_url': response[1]})
        else:
            return Response({'error': response[1]})

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(GDSFileViewSet, self).handle_exception(exc)
