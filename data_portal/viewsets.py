import logging

from django.db import InternalError
from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet, ViewSet

from utils import libs3
from .models import LIMSRow, S3Object
from .pagination import StandardResultsSetPagination
from .serializers import LIMSRowModelSerializer, S3ObjectModelSerializer, SubjectIdSerializer, RunIdSerializer, \
    BucketIdSerializer

logger = logging.getLogger()

LIMS_SEARCH_ORDER_FIELDS = [
    'subject_id', 'timestamp', 'type', 'run', 'sample_id', 'external_subject_id', 'results', 'phenotype',
    'library_id', 'external_sample_id', 'project_name', 'illumina_id',
]


def presign_response(bucket, key):
    response = libs3.presign_s3_file(bucket, key)
    if response[0]:
        return Response({'signed_url': response[1]})
    else:
        return Response({'error': response[1]})


class LIMSRowViewSet(ReadOnlyModelViewSet):
    queryset = LIMSRow.objects.all()
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = LIMS_SEARCH_ORDER_FIELDS
    ordering = ['-subject_id']
    search_fields = ordering_fields


class S3ObjectViewSet(ReadOnlyModelViewSet):
    queryset = S3Object.objects.get_all()
    serializer_class = S3ObjectModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['key']
    search_fields = ['$key']

    @action(detail=True)
    def presign(self, request, pk=None):
        obj: S3Object = self.get_object()
        return presign_response(obj.bucket, obj.key)

    @action(detail=True)
    def status(self, request, pk=None):
        obj: S3Object = self.get_object()
        response = libs3.head_s3_object(obj.bucket, obj.key)
        obj_status = S3ObjectModelSerializer(obj).data
        obj_status.update(head_object=response[1])
        return Response(obj_status)

    @action(detail=True)
    def restore(self, request, pk=None):
        query_params = self.request.query_params
        days = query_params.get('days', None)

        if days is not None and not days.isnumeric():
            return Response({'error': 'Error in days request parameter'})

        # TODO demo mode
        __demo__ = True
        if __demo__:
            return Response({'days': days})

        obj: S3Object = self.get_object()

        if days is None:
            response = libs3.restore_s3_object(obj.bucket, obj.key)
        else:
            response = libs3.restore_s3_object(obj.bucket, obj.key, days=days)

        return Response(response[1])


class BucketViewSet(ReadOnlyModelViewSet):
    queryset = S3Object.objects.values_list('bucket', named=True).order_by('bucket').distinct()
    serializer_class = BucketIdSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['bucket']
    ordering = ['-bucket']
    search_fields = ordering_fields


class SubjectViewSet(ReadOnlyModelViewSet):
    queryset = LIMSRow.objects.values_list('subject_id', named=True).filter(subject_id__isnull=False).distinct()
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
        bucket = self.request.query_params.get('bucket', None)
        return S3Object.objects.get_by_subject_id(self.kwargs['subject_pk'], bucket=bucket)

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(SubjectS3ObjectViewSet, self).handle_exception(exc)


class RunViewSet(ReadOnlyModelViewSet):
    queryset = LIMSRow.objects.values_list('illumina_id', named=True).filter(illumina_id__isnull=False).distinct()
    serializer_class = RunIdSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['illumina_id']
    ordering = ['-illumina_id']
    search_fields = ordering_fields

    def retrieve(self, request, pk=None, **kwargs):
        data = {
            'id': pk,
            'lims': {
                'count': LIMSRow.objects.filter(illumina_id=pk).count()
            },
            's3': {
                'count': S3Object.objects.get_by_illumina_id(pk).count()
            }
        }
        return Response(data)


class RunDataViewSet(ReadOnlyModelViewSet):
    serializer_class = S3ObjectModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['key']
    search_fields = ['$key']

    def get_queryset(self):
        bucket = self.request.query_params.get('bucket', None)
        return S3Object.objects.get_by_illumina_id(self.kwargs['run_pk'], bucket=bucket)

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(RunDataViewSet, self).handle_exception(exc)


class RunMetaDataViewSet(ReadOnlyModelViewSet):
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = LIMS_SEARCH_ORDER_FIELDS
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        return LIMSRow.objects.filter(illumina_id=self.kwargs['run_pk'])


class PresignedUrlViewSet(ViewSet):

    def list(self, request):
        """
        TODO this could extend to support possibly presign list of objects
        :param request:
        :return:
        """
        query_params = self.request.query_params
        bucket = query_params.get('bucket', None)
        key = query_params.get('key', None)

        if bucket is None or key is None:
            return Response({'error': 'Missing required parameters: bucket or key'})

        return presign_response(bucket, key)
