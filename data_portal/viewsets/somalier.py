# -*- coding: utf-8 -*-
"""somalier endpoints

NOTE:
     This is DRF based Portal API impls.
"""
import logging

from rest_framework import parsers
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet

from data_processors.pipeline.domain.somalier import HolmesProxyImpl

logger = logging.getLogger()


class SomalierViewSet(ViewSet):
    """Experimental endpoint for exposing Somalier through Holmes interfaces"""
    parser_classes = [parsers.JSONParser]

    def list(self, request):
        """You have hit GET /somalier. This is placeholder only list endpoint. Nothing here yet.
        Please use POST /somalier endpoint which do Holmes check projection by default.
        """
        logger.debug(self.request.query_params)
        return Response(data={
            'list': "somalier",
            'message': self.list.__doc__
        })

    def create(self, request):
        """POST /somalier endpoint which do Holmes check projection by default"""
        return self.check(request)

    @action(detail=False, methods=['post'])
    def check(self, request):
        payload = self.request.data
        # TODO just return check execution_result as-is for now
        #  we can improve API response with more deterministic Responses
        #  such as 200 or 400 by checking execution_result['status']
        #  and with custom Serializer for response model e.g. see LabMetadataSyncSerializer
        output = HolmesProxyImpl(payload).check().output
        return Response(data=output)

    @action(detail=False, methods=['post'])
    def extract(self, request):
        payload = self.request.data
        output = HolmesProxyImpl(payload).extract().output
        return Response(data=output)
