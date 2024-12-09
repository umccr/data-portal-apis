# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from django.db.models import QuerySet
from libumccr.aws import libs3
from rest_framework import filters
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from data_portal.models.analysisresult import AnalysisResult, Lookup, PlatformGeneration, AnalysisMethod
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
        results = []
        results_gds = []
        results_sash = []
        results_icav1_cttsov1 = []
        results_icav1_wgts = []
        results_icav2_cttsov2 = []
        results_icav2_wgts = []
        results_icav2_sash = []

        qs: QuerySet = AnalysisResult.objects.get_by_lookup(Lookup(pk, PlatformGeneration.ONE)).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results = r.s3objects.all()

        qs: QuerySet = AnalysisResult.objects.get_by_lookup(Lookup(pk, PlatformGeneration.TWO)).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results_gds = r.gdsfiles.all()
            results_sash = r.s3objects.all()

        results_icav1_cttsov1_lookup = Lookup(key=pk, gen=PlatformGeneration.TWO, method=AnalysisMethod.TSO500)
        qs: QuerySet = AnalysisResult.objects.get_by_lookup(results_icav1_cttsov1_lookup).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results_icav1_cttsov1 = r.s3objects.all()

        results_icav1_wgts_lookup = Lookup(key=pk, gen=PlatformGeneration.TWO, method=AnalysisMethod.WGTS)
        qs: QuerySet = AnalysisResult.objects.get_by_lookup(results_icav1_wgts_lookup).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results_icav1_wgts = r.s3objects.all()

        results_byob_cttsov2_lookup = Lookup(key=pk, gen=PlatformGeneration.THREE, method=AnalysisMethod.TSO500V2)
        qs: QuerySet = AnalysisResult.objects.get_by_lookup(results_byob_cttsov2_lookup).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results_icav2_cttsov2 = r.s3objects.all()

        results_byob_wgts_lookup = Lookup(key=pk, gen=PlatformGeneration.THREE, method=AnalysisMethod.WGTS)
        qs: QuerySet = AnalysisResult.objects.get_by_lookup(results_byob_wgts_lookup).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results_icav2_wgts = r.s3objects.all()

        results_byob_sash_lookup = Lookup(key=pk, gen=PlatformGeneration.THREE, method=AnalysisMethod.SASH)
        qs: QuerySet = AnalysisResult.objects.get_by_lookup(results_byob_sash_lookup).prefetch_related()
        if qs.exists():
            r: AnalysisResult = qs.get()
            results_icav2_sash = r.s3objects.all()

        features = []

        def _collect_features(_qs: QuerySet):
            for obj in _qs:
                o: S3Object = obj
                if o.key.endswith('png'):
                    # features.append(S3ObjectModelSerializer(o).data)
                    resp = libs3.presign_s3_file(o.bucket, o.key)
                    if resp[0]:
                        features.append(resp[1])

        _collect_features(results)
        _collect_features(results_icav1_wgts)
        _collect_features(results_icav2_wgts)

        data = {'id': pk}
        data.update(lims=LIMSRowModelSerializer(LIMSRow.objects.filter(subject_id=pk), many=True).data)
        data.update(features=features)
        data.update(results=S3ObjectModelSerializer(results, many=True).data)
        data.update(results_sash=S3ObjectModelSerializer(results_sash, many=True).data)
        data.update(results_gds=GDSFileModelSerializer(results_gds, many=True).data)
        data.update(results_icav1_cttsov1=S3ObjectModelSerializer(results_icav1_cttsov1, many=True).data)
        data.update(results_icav1_wgts=S3ObjectModelSerializer(results_icav1_wgts, many=True).data)
        data.update(results_icav2_cttsov2=S3ObjectModelSerializer(results_icav2_cttsov2, many=True).data)
        data.update(results_icav2_wgts=S3ObjectModelSerializer(results_icav2_wgts, many=True).data)
        data.update(results_icav2_sash=S3ObjectModelSerializer(results_icav2_sash, many=True).data)

        return Response(data)
