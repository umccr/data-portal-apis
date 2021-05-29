import logging
from datetime import datetime

from django.db import InternalError
from django.utils.http import parse_http_date_safe
from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet, ViewSet

from utils import libs3, libjson, ica
from .models import LIMSRow, S3Object, GDSFile, Report, LabMetadata, FastqListRow, SequenceRun, Workflow
from .pagination import StandardResultsSetPagination
from .renderers import content_renderers
from .serializers import LIMSRowModelSerializer, LabMetadataModelSerializer, S3ObjectModelSerializer, \
    SubjectIdSerializer, RunIdSerializer, BucketIdSerializer, GDSFileModelSerializer, ReportSerializer, \
    FastqListRowSerializer, SequenceRunSerializer, WorkflowSerializer

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
    serializer_class = LIMSRowModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = LIMS_SEARCH_ORDER_FIELDS
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        subject = self.request.query_params.get('subject', None)
        run = self.request.query_params.get('run', None)
        return LIMSRow.objects.get_by_keyword(subject=subject, run=run)


class LabMetadataViewSet(ReadOnlyModelViewSet):
    queryset = LabMetadata.objects.all()
    serializer_class = LabMetadataModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['library_id']
    search_fields = ordering_fields


class S3ObjectViewSet(ReadOnlyModelViewSet):
    serializer_class = S3ObjectModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['key']
    search_fields = ['$key']

    def get_queryset(self):
        bucket = self.request.query_params.get('bucket', None)
        subject = self.request.query_params.get('subject', None)
        run = self.request.query_params.get('run', None)
        return S3Object.objects.get_by_keyword(bucket=bucket, subject=subject, run=run)

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

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(S3ObjectViewSet, self).handle_exception(exc)


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
        response = ica.presign_gds_file(file_id=obj.file_id, volume_name=obj.volume_name, path_=obj.path)
        if response[0]:
            return Response({'signed_url': response[1]})
        else:
            return Response({'error': response[1]})

    def handle_exception(self, exc):
        logger.exception(exc)
        if isinstance(exc, InternalError):
            return Response({'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return super(GDSFileViewSet, self).handle_exception(exc)


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
        data.update(features=features)
        data.update(results=S3ObjectModelSerializer(results, many=True).data)
        return Response(data)


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
        }
        return Response(data)


class ReportViewSet(ReadOnlyModelViewSet):
    queryset = Report.objects.all()
    serializer_class = ReportSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-subject_id']
    search_fields = ordering_fields


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


class FastqListRowViewSet(ReadOnlyModelViewSet):
    serializer_class = FastqListRowSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['id', 'rgid', 'rgsm', 'rglb', 'lane']
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        sequence_run = self.request.query_params.get('sequence_run', None)
        sequence = self.request.query_params.get('sequence', None)
        run = self.request.query_params.get('run', None)
        rgid = self.request.query_params.get('rgid', None)
        rgsm = self.request.query_params.get('rgsm', None)
        rglb = self.request.query_params.get('rglb', None)
        lane = self.request.query_params.get('lane', None)
        return FastqListRow.objects.get_by_keyword(
            sequence_run=sequence_run,
            sequence=sequence,
            run=run,
            rgid=rgid,
            rgsm=rgsm,
            rglb=rglb,
            lane=lane,
        )


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


class WorkflowViewSet(ReadOnlyModelViewSet):
    serializer_class = WorkflowSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['id', 'wfr_name', 'sample_name', 'type_name', 'wfr_id', 'version', 'start', 'end', 'end_status']
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        sequence_run = self.request.query_params.get('sequence_run', None)
        sequence = self.request.query_params.get('sequence', None)
        run = self.request.query_params.get('run', None)
        sample_name = self.request.query_params.get('sample_name', None)
        type_name = self.request.query_params.get('type_name', None)
        end_status = self.request.query_params.get('end_status', None)
        return Workflow.objects.get_by_keyword(
            sequence_run=sequence_run,
            sequence=sequence,
            run=run,
            sample_name=sample_name,
            type_name=type_name,
            end_status=end_status,
        )
