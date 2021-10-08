# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging
from datetime import datetime
from collections import defaultdict

from django.db import InternalError
from django.utils.http import parse_http_date_safe
from rest_framework import filters, status, parsers
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet, ViewSet

from data_processors.pipeline.domain.pairing import TNPairing
from utils import libs3, libjson, ica, gds
from .models import LIMSRow, S3Object, GDSFile, Report, LabMetadata, FastqListRow, SequenceRun, \
    Workflow, LibraryRun, Sequence
from .pagination import StandardResultsSetPagination
from .renderers import content_renderers
from .serializers import LIMSRowModelSerializer, LabMetadataModelSerializer, S3ObjectModelSerializer, \
    SubjectIdSerializer, RunIdSerializer, BucketIdSerializer, GDSFileModelSerializer, ReportSerializer, \
    FastqListRowSerializer, SequenceRunSerializer, WorkflowSerializer, LibraryRunModelSerializer, SequenceSerializer

logger = logging.getLogger()

LIMS_SEARCH_ORDER_FIELDS = [
    'subject_id', 'timestamp', 'type', 'run', 'sample_id', 'external_subject_id', 'results', 'phenotype',
    'library_id', 'external_sample_id', 'project_name', 'illumina_id',
]

METADATA_SEARCH_ORDER_FIELDS = [
    'library_id', 'sample_name', 'sample_id', 'external_sample_id', 'subject_id', 'external_subject_id', 'phenotype',
    'quality', 'source', 'project_name', 'project_owner', 'experiment_id', 'type', 'assay', 'workflow',
]


def _error_response(message, status_code=400, err=None) -> Response:
    data = {'error': message}
    if err:
        data['detail'] = err
    return Response(
        data=data,
        status=status_code
    )


def _presign_response(bucket, key) -> Response:
    response = libs3.presign_s3_file(bucket, key)
    if response[0]:
        return Response({'signed_url': response[1]})
    else:
        return Response({'error': response[1]})


def _presign_list_response(presigned_urls: list) -> Response:
    if presigned_urls and len(presigned_urls) > 0:
        return Response({'signed_urls': presigned_urls})
    else:
        return _error_response(message="No presigned URLs to return.")
    pass


def _gds_file_recs_to_presign_resps(gds_records: list) -> list:
    resps = list()
    for rec in gds_records:
        resps.append(_gds_file_rec_to_presign_resp(rec))
    return resps


def _gds_file_rec_to_presign_resp(gds_file_response) -> dict:
    return {
        'volume': gds_file_response.volume_name,
        'path': gds_file_response.path,
        'presigned_url': gds_file_response.presigned_url
    }


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
    serializer_class = LabMetadataModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = METADATA_SEARCH_ORDER_FIELDS
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        subject = self.request.query_params.get('subject', None)
        sample = self.request.query_params.get('sample', None)
        library = self.request.query_params.get('library', None)
        phenotype = self.request.query_params.get('phenotype', None)
        type_ = self.request.query_params.get('type', None)
        project_owner = self.request.query_params.get('project_owner', None)
        project_name = self.request.query_params.get('project_name', None)

        return LabMetadata.objects.get_by_keyword(
            subject_id=subject,
            sample_id=sample,
            library_id=library,
            phenotype=phenotype,
            type=type_,
            project_owner=project_owner,
            project_name=project_name,
        )


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
    serializer_class = ReportSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = ['subject_id', 'sample_id', 'library_id', 'type']
    ordering = ['-subject_id']
    search_fields = ordering_fields

    def get_queryset(self):
        subject = self.request.query_params.get('subject', None)
        sample = self.request.query_params.get('sample', None)
        library = self.request.query_params.get('library', None)
        type_ = self.request.query_params.get('type', None)

        return Report.objects.get_by_keyword(
            subject=subject,
            sample=sample,
            library=library,
            type=type_,
        )


class PresignedUrlViewSet(ViewSet):

    def create(self, request):
        # payload is expected to be simple list of gds://... urls
        payload = request.data
        # TODO: check payload and filter/report unrecognised/unsupported URLs

        # parse file GDS urls into volume and path components
        vol_path = defaultdict(list)
        try:
            for entry in payload:
                volume, path = gds.parse_path(entry)
                vol_path[volume].append(path)
        except Exception as ex:
            return _error_response(message="Could not parse GDS URL.", err=ex)

        presign_list = list()
        try:
            for vol in vol_path.keys():
                tmp_list = gds.get_files_list(volume_name=vol, paths=vol_path[vol])
                if tmp_list:
                    presign_list.extend(tmp_list)
        except Exception as ex:
            return _error_response(message="Could create presigned URL.", err=ex)

        if len(presign_list) < 1:
            return _error_response(message="No matching GDS records found.", status_code=404)

        # Convert List of libgds.FileResponse objects into response objects
        try:
            resps = _gds_file_recs_to_presign_resps(presign_list)
        except Exception as ex:
            return _error_response(message="Could create presigned URL.", err=ex)

        # wrap response objects in rest framework Response object
        return _presign_list_response(presigned_urls=resps)

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
        run = self.request.query_params.get('run', None)
        rgid = self.request.query_params.get('rgid', None)
        rgsm = self.request.query_params.get('rgsm', None)
        rglb = self.request.query_params.get('rglb', None)
        lane = self.request.query_params.get('lane', None)
        project_owner = self.request.query_params.get('project_owner', None)

        return FastqListRow.objects.get_by_keyword(
            run=run,
            rgid=rgid,
            rgsm=rgsm,
            rglb=rglb,
            lane=lane,
            project_owner=project_owner
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


class PairingViewSet(ViewSet):
    """Experimental endpoint for exposing workflow T/N pairing"""
    parser_classes = [parsers.JSONParser]

    def list(self, request):
        """You have hit GET /pairing. This is placeholder only list endpoint. Nothing here yet.
        Please use POST /pairing endpoint which is by_sequence_runs projection by default.
        """
        logger.debug(self.request.query_params)
        return Response(data={
            'list': "pairing",
            'message': self.list.__doc__
        })

    def create(self, request):
        """POST /pairing endpoint which is by_sequence_runs projection by default"""
        return self.by_sequence_runs(request)

    @action(detail=False, methods=['post'])
    def by_sequence_runs(self, request):
        seq_run_list = self.request.data
        tn_pairing = TNPairing()
        for seq_run in seq_run_list:
            tn_pairing.add_sequence_run(instrument_run_id=seq_run)
        tn_pairing.by_sequence_runs()
        return Response(data=tn_pairing.job_list)

    @action(detail=False, methods=['post'])
    def by_workflows(self, request):
        qc_workflow_list = self.request.data
        tn_pairing = TNPairing()
        for qc_workflow in qc_workflow_list:
            tn_pairing.add_workflow(wfr_id=qc_workflow)
        tn_pairing.by_workflows()
        return Response(data=tn_pairing.job_list)

    @action(detail=False, methods=['post'])
    def by_subjects(self, request):
        subject_id_list = self.request.data
        tn_pairing = TNPairing()
        for subject_id in subject_id_list:
            tn_pairing.add_subject(subject_id=subject_id)
        tn_pairing.by_subjects()
        return Response(data=tn_pairing.job_list)

    @action(detail=False, methods=['post'])
    def by_libraries(self, request):
        library_id_list = self.request.data
        tn_pairing = TNPairing()
        for library_id in library_id_list:
            tn_pairing.add_library(library_id=library_id)
        tn_pairing.by_libraries()
        return Response(data=tn_pairing.job_list)

    @action(detail=False, methods=['post'])
    def by_samples(self, request):
        sample_id_list = self.request.data
        tn_pairing = TNPairing()
        for sample_id in sample_id_list:
            tn_pairing.add_sample(sample_id=sample_id)
        tn_pairing.by_samples()
        return Response(data=tn_pairing.job_list)


class SequenceViewSet(ReadOnlyModelViewSet):
    serializer_class = SequenceSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        instrument_run_id = self.request.query_params.get('instrument_run_id', None)
        run_id = self.request.query_params.get('run_id', None)
        sample_sheet_name = self.request.query_params.get('sample_sheet_name', None)
        gds_folder_path = self.request.query_params.get('gds_folder_path', None)
        gds_volume_name = self.request.query_params.get('gds_volume_name', None)
        reagent_barcode = self.request.query_params.get('reagent_barcode', None)
        flowcell_barcode = self.request.query_params.get('flowcell_barcode', None)
        status_ = self.request.query_params.get('status', None)
        start_time = self.request.query_params.get('start_time', None)
        end_time = self.request.query_params.get('end_time', None)

        return Sequence.objects.get_by_keyword(
            instrument_run_id=instrument_run_id,
            run_id=run_id,
            sample_sheet_name=sample_sheet_name,
            gds_folder_path=gds_folder_path,
            gds_volume_name=gds_volume_name,
            reagent_barcode=reagent_barcode,
            flowcell_barcode=flowcell_barcode,
            status=status_,
            start_time=start_time,
            end_time=end_time,
        )


class LibraryRunViewSet(ReadOnlyModelViewSet):
    serializer_class = LibraryRunModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-id']
    search_fields = ordering_fields

    def get_queryset(self):
        library_id = self.request.query_params.get('library_id', None)
        instrument_run_id = self.request.query_params.get('instrument_run_id', None)
        run_id = self.request.query_params.get('run_id', None)
        lane = self.request.query_params.get('lane', None)
        override_cycles = self.request.query_params.get('override_cycles', None)
        coverage_yield = self.request.query_params.get('coverage_yield', None)
        qc_pass = self.request.query_params.get('qc_pass', None)
        qc_status = self.request.query_params.get('qc_status', None)
        valid_for_analysis = self.request.query_params.get('valid_for_analysis', None)

        return LibraryRun.objects.get_by_keyword(
            library_id=library_id,
            instrument_run_id=instrument_run_id,
            run_id=run_id,
            lane=lane,
            override_cycles=override_cycles,
            coverage_yield=coverage_yield,
            qc_pass=qc_pass,
            qc_status=qc_status,
            valid_for_analysis=valid_for_analysis,
        )
