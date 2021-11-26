# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from libumccr.aws import libs3
from rest_framework import filters
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.gdsfile import GDSFile
from data_portal.models.limsrow import LIMSRow
from data_portal.models.s3object import S3Object
from data_portal.pagination import StandardResultsSetPagination
from data_portal.serializers import LIMSRowModelSerializer, S3ObjectModelSerializer, SubjectIdSerializer, \
    GDSFileModelSerializer

logger = logging.getLogger()


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
        results_gds = GDSFile.objects.get_subject_results(pk).all()

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
        data.update(results_gds=GDSFileModelSerializer(results_gds, many=True).data)
        return Response(data)
