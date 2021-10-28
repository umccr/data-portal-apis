# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging
from datetime import datetime

from django.db import InternalError
from django.utils.http import parse_http_date_safe

from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.s3object import S3Object
from data_portal.pagination import StandardResultsSetPagination
from data_portal.renderers import content_renderers
from data_portal.serializers import S3ObjectModelSerializer
from data_portal.viewsets.utils import _presign_response

from utils import libs3

logger = logging.getLogger()

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
