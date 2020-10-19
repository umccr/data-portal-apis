import logging
from datetime import datetime

from django.db import InternalError
from django.utils.http import parse_http_date_safe
from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet, ViewSet

from utils import libs3, libjson
from .models import LIMSRow, S3Object, GDSFile
from .pagination import StandardResultsSetPagination
from .renderers import content_renderers
from .serializers import LIMSRowModelSerializer, S3ObjectModelSerializer, SubjectIdSerializer, RunIdSerializer, \
    BucketIdSerializer, GDSFileModelSerializer

logger = logging.getLogger()

LIMS_SEARCH_ORDER_FIELDS = [
    'subject_id', 'timestamp', 'type', 'run', 'sample_id', 'external_subject_id', 'results', 'phenotype',
    'library_id', 'external_sample_id', 'project_name', 'illumina_id',
]


def _presign_response(bucket, key):
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
        return _presign_response(obj.bucket, obj.key)

    @action(detail=True)
    def status(self, request, pk=None):
        obj: S3Object = self.get_object()
        response = libs3.head_s3_object(obj.bucket, obj.key)
        obj_status = S3ObjectModelSerializer(obj).data
        obj_status.update(head_object=response[1])
        return Response(obj_status)

    @action(detail=True, methods=['post'])
    def restore(self, request, pk=None):
        payload = self.request.data
        obj: S3Object = self.get_object()
        response = libs3.restore_s3_object(obj.bucket, obj.key, days=payload['days'], email=payload['email'])
        return Response(response[1])

    @action(detail=True, methods=['get'], renderer_classes=content_renderers)
    def content(self, request, pk=None):
        request_headers = {}

        range_header = request.META.get('HTTP_RANGE', None)
        if range_header:
            request_headers.update(Range=range_header)

        version_id = request.META.get('HTTP_VERSIONID', None)
        if version_id:
            request_headers.update(VersionId=version_id)

        if_match = request.META.get('HTTP_IF_MATCH', None)
        if if_match:
            request_headers.update(IfMatch=if_match)

        if_none_match = request.META.get('HTTP_IF_NONE_MATCH', None)
        if if_none_match:
            request_headers.update(IfNoneMatch=if_none_match)

        if_modified_since = request.META.get('HTTP_IF_MODIFIED_SINCE', None)
        if if_modified_since:
            if_modified_since = parse_http_date_safe(if_modified_since)
            if if_modified_since:
                request_headers.update(IfModifiedSince=datetime.fromtimestamp(if_modified_since))

        if_unmodified_since = request.META.get('HTTP_IF_UNMODIFIED_SINCE', None)
        if if_unmodified_since:
            if_unmodified_since = parse_http_date_safe(if_unmodified_since)
            if if_unmodified_since:
                request_headers.update(IfUnmodifiedSince=datetime.fromtimestamp(if_unmodified_since))

        obj: S3Object = self.get_object()

        if len(request_headers) == 0:
            resp = libs3.get_s3_object(obj.bucket, obj.key)
        else:
            resp = libs3.get_s3_object(obj.bucket, obj.key, **request_headers)

        resp_ok = resp[0]
        resp_data = resp[1]

        if resp_ok:
            response_headers = resp_data['ResponseMetadata']['HTTPHeaders']

            if resp_data.get('Error'):
                error_code = resp_data['Error']['Code']
                if error_code == "304":
                    return Response(headers=response_headers, status=status.HTTP_304_NOT_MODIFIED)

            content_type = response_headers['content-type']
            if content_type is None:
                content_type = 'application/octet-stream'
            body = resp_data['Body']
            return Response(body, headers=response_headers, content_type=content_type)

        else:
            return Response(libjson.dumps(resp_data), content_type='application/json')


class GDSFileViewSet(ReadOnlyModelViewSet):
    queryset = GDSFile.objects.get_all()
    serializer_class = GDSFileModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['path']
    search_fields = ['$path']

    # TODO client direct API request to GDS endpoint?
    # @action(detail=True)
    # def presign(self, request, pk=None):
    #     obj: GDSFile = self.get_object()
    #     response = libgds.get_file(obj.file_id)
    #     if response[0]:
    #         return Response({'signed_url': response[1]})
    #     else:
    #         return Response({'error': response[1]})


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

    def _base_url(self, data_lake):
        abs_uri = self.request.build_absolute_uri()
        return abs_uri + data_lake if abs_uri.endswith('/') else abs_uri + f"/{data_lake}"

    def retrieve(self, request, pk=None, **kwargs):
        results = S3Object.objects.get_subject_results(pk).all()

        features = []
        for obj in results:
            o: S3Object = obj
            if o.key.endswith('png'):
                # features.append(S3ObjectModelSerializer(o).data)
                resp = libs3.presign_s3_file(o.bucket, o.key)
                if resp[0]:
                    features.append(resp[1])

        data = {'id': pk}
        data.update(lims=LIMSRowModelSerializer(LIMSRow.objects.filter(subject_id=pk), many=True).data)

        data.update(
            s3={
                'count': S3Object.objects.get_by_subject_id(pk).count(),
                'next': self._base_url('s3')
            })
        data.update(
            gds={
                'count': GDSFile.objects.get_by_subject_id(pk).count(),
                'next': self._base_url('gds')
            })
        data.update(features=features)
        data.update(results=S3ObjectModelSerializer(results, many=True).data)
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


class SubjectGDSFileViewSet(ReadOnlyModelViewSet):
    serializer_class = GDSFileModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['path']
    search_fields = ['$path']

    def get_queryset(self):
        volume_name = self.request.query_params.get('volume_name', None)
        return GDSFile.objects.get_by_subject_id(self.kwargs['subject_pk'], volume_name=volume_name)

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(SubjectGDSFileViewSet, self).handle_exception(exc)


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
            },
            'gds': {
                'count': GDSFile.objects.get_by_illumina_id(pk).count()
            },
        }
        return Response(data)


class RunDataS3ObjectViewSet(ReadOnlyModelViewSet):
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

        return super(RunDataS3ObjectViewSet, self).handle_exception(exc)


class RunDataGDSFileViewSet(ReadOnlyModelViewSet):
    serializer_class = GDSFileModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['path']
    search_fields = ['$path']

    def get_queryset(self):
        volume_name = self.request.query_params.get('volume_name', None)
        return GDSFile.objects.get_by_illumina_id(self.kwargs['run_pk'], volume_name=volume_name)

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(RunDataGDSFileViewSet, self).handle_exception(exc)


class RunDataLIMSViewSet(ReadOnlyModelViewSet):
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

        return _presign_response(bucket, key)
