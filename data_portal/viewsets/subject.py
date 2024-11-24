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
        # TODO replacing with pre-build AnalysisResult index table for better performance
        # results = S3Object.objects.get_subject_results(pk).all()
        # results_gds = GDSFile.objects.get_subject_results(pk).all()
        # results_sash = S3Object.objects.get_subject_sash_results(pk).all()
        # results_byob_cttsov2 = S3Object.objects.get_subject_cttsov2_results_from_byob(pk).all()
        # results_byob_wgts = S3Object.objects.get_subject_wgts_results_from_byob(pk).all()
        # results_byob_sash = S3Object.objects.get_subject_sash_results_from_byob(pk).all()

        results = []
        results_gds = []
        results_sash = []
        results_byob_cttsov2 = []
        results_byob_wgts = []
        results_byob_sash = []

        qs1: QuerySet = AnalysisResult.objects.get_by_lookup(Lookup(pk, PlatformGeneration.ONE)).prefetch_related()
        if qs1.exists():
            r1: AnalysisResult = qs1.get()
            results = r1.s3objects.all()

        qs2: QuerySet = AnalysisResult.objects.get_by_lookup(Lookup(pk, PlatformGeneration.TWO)).prefetch_related()
        if qs2.exists():
            r2: AnalysisResult = qs2.get()
            results_gds = r2.gdsfiles.all()
            results_sash = r2.s3objects.all()

        results_byob_cttsov2_lookup = Lookup(key=pk, gen=PlatformGeneration.THREE, method=AnalysisMethod.TSO500V2)
        qs3: QuerySet = AnalysisResult.objects.get_by_lookup(results_byob_cttsov2_lookup).prefetch_related()
        if qs3.exists():
            r3: AnalysisResult = qs3.get()
            results_byob_cttsov2 = r3.s3objects.all()

        results_byob_wgts_lookup = Lookup(key=pk, gen=PlatformGeneration.THREE, method=AnalysisMethod.WGTS)
        qs4: QuerySet = AnalysisResult.objects.get_by_lookup(results_byob_wgts_lookup).prefetch_related()
        if qs4.exists():
            r4: AnalysisResult = qs4.get()
            results_byob_wgts = r4.s3objects.all()

        results_byob_sash_lookup = Lookup(key=pk, gen=PlatformGeneration.THREE, method=AnalysisMethod.SASH)
        qs5: QuerySet = AnalysisResult.objects.get_by_lookup(results_byob_sash_lookup).prefetch_related()
        if qs5.exists():
            r5: AnalysisResult = qs5.get()
            results_byob_sash = r5.s3objects.all()

        features = []

        def _collect_features(qs: QuerySet):
            for obj in qs:
                o: S3Object = obj
                if o.key.endswith('png'):
                    # features.append(S3ObjectModelSerializer(o).data)
                    resp = libs3.presign_s3_file(o.bucket, o.key)
                    if resp[0]:
                        features.append(resp[1])

        _collect_features(results)
        _collect_features(results_byob_wgts)

        data = {'id': pk}
        data.update(lims=LIMSRowModelSerializer(LIMSRow.objects.filter(subject_id=pk), many=True).data)
        data.update(features=features)
        data.update(results=S3ObjectModelSerializer(results, many=True).data)
        data.update(results_sash=S3ObjectModelSerializer(results_sash, many=True).data)
        data.update(results_gds=GDSFileModelSerializer(results_gds, many=True).data)
        data.update(results_byob_cttsov2=S3ObjectModelSerializer(results_byob_cttsov2, many=True).data)
        data.update(results_byob_wgts=S3ObjectModelSerializer(results_byob_wgts, many=True).data)
        data.update(results_byob_sash=S3ObjectModelSerializer(results_byob_sash, many=True).data)

        return Response(data)
