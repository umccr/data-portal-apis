# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import parsers
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet

from data_processors.pipeline.domain.somalier_check import SomalierCheck

logger = logging.getLogger()


class SomalierCheckViewSet(ViewSet):
    """Experimental endpoint for exposing somalier check lambda"""
    parser_classes = [parsers.JSONParser]

    def create(self, request):
        """POST /pairing endpoint which is by_sequence_runs projection by default"""
        return self.run_somalier_check(request)

    @action(detail=False, methods=['post'])
    def run_somalier_check(self, request):
        somalier_check_data = self.request.data
        somalier_check = SomalierCheck(somalier_check_data)
        return Response(data=somalier_check.get_related_bams())
