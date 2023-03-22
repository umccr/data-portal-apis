# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging
import os

from libumccr import libjson, aws
from libumccr.aws.liblambda import LambdaInvocationType
from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.labmetadata import LabMetadata
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import LabMetadataModelSerializer, LabMetadataSyncSerializer

logger = logging.getLogger(__name__)

allowed_fields = ['project_name', 'project_owner', 'workflow', 'source', 'assay', 'type', 'phenotype']


class LabMetadataViewSet(ReadOnlyModelViewSet):
    serializer_class = LabMetadataModelSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [filters.OrderingFilter, filters.SearchFilter]
    ordering_fields = '__all__'
    ordering = ['-subject_id']
    search_fields = LabMetadata.get_base_fields()

    def get_queryset(self):
        return LabMetadata.objects.get_by_keyword(**self.request.query_params)

    @action(detail=False, methods=['post'], serializer_class=LabMetadataSyncSerializer)
    def sync(self, request):

        fn_name = os.getenv('LAB_METADATA_SYNC_LAMBDA', "data-portal-api-dev-labmetadata_scheduled_update_processor")

        try:
            payload = self.request.data if self.request.data is not None else {}
            payload_json = libjson.dumps(payload)

            logger.debug(payload_json)

            client = aws.lambda_client()
            lmbda_response = client.invoke(
                FunctionName=fn_name,
                InvocationType=LambdaInvocationType.EVENT.value,
                Payload=payload_json,
            )

            logger.debug(lmbda_response)

            lmbda_details = {
                'function': fn_name,
                'payload': payload,
                'status': lmbda_response['StatusCode'],
                'response': lmbda_response['ResponseMetadata'],
            }

            logger.info(libjson.dumps(lmbda_details))

            return Response(data=payload, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(e)
            return Response(data={'message': "INTERNAL_SERVER_ERROR"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=['get'])
    def by_aggregate_count(self, request):
        fields = self.request.query_params.getlist('fields', None)

        if fields is None or not fields:
            return Response(data={})

        if 'all' in fields:
            fields.clear()
            fields.extend(allowed_fields)

        fields = list(set(fields) & set(allowed_fields))  # intersect

        data = {}
        for f in fields:
            data.update({
                str(f): LabMetadata.objects.get_by_aggregate_count(str(f))
            })

        return Response(data=data)

    @action(detail=False, methods=['get'])
    def by_cube(self, request):
        fields = self.request.query_params.getlist('fields', None)

        if fields is None or not fields or len(fields) < 2:
            return Response(data={})

        if not all(item in allowed_fields for item in fields):
            return Response(data={})

        data = LabMetadata.objects.get_by_cube(field_left=fields[0], field_right=fields[1], field_sort=fields[0])

        return Response(data=data)
