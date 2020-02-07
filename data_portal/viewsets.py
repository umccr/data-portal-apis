import logging

from django.db import InternalError
from rest_framework import filters, status
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from .models import LIMSRow, S3Object
from .pagination import StandardResultsSetPagination
from .serializers import LIMSRowModelSerializer, S3ObjectModelSerializer, SubjectIdSerializer

logger = logging.getLogger()


class LIMSRowViewSet(ReadOnlyModelViewSet):
    queryset = LIMSRow.objects.all()
    logger.debug('Query to be executed: %s ' % queryset.query)
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = [
        'subject_id', 'timestamp', 'type', 'run', 'sample_id', 'external_subject_id', 'results', 'phenotype',
        'library_id', 'external_sample_id', 'project_name', 'illumina_id',
    ]
    ordering = ['-subject_id']
    search_fields = ordering_fields


class S3ObjectViewSet(ReadOnlyModelViewSet):
    queryset = S3Object.objects.get_all()
    logger.debug('Query to be executed: %s ' % queryset.query)
    serializer_class = S3ObjectModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['key']
    search_fields = ['key']


class SubjectViewSet(ReadOnlyModelViewSet):
    queryset = LIMSRow.objects.values_list('subject_id', named=True).filter(subject_id__isnull=False).distinct()
    logger.debug('Query to be executed: %s ' % queryset.query)
    serializer_class = SubjectIdSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['subject_id']
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def retrieve(self, request, pk=None, **kwargs):
        data = {
            'id': pk,
            'lims': LIMSRowModelSerializer(LIMSRow.objects.filter(subject_id=pk), many=True).data,
            's3': {
                'count': S3Object.objects.get_by_subject_id(pk).count()
            }
        }
        return Response(data)


class SubjectS3ObjectViewSet(ReadOnlyModelViewSet):
    serializer_class = S3ObjectModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['key']
    search_fields = ['$key']

    def get_queryset(self):
        return S3Object.objects.get_by_subject_id(self.kwargs['subject_pk'])

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(SubjectS3ObjectViewSet, self).handle_exception(exc)
