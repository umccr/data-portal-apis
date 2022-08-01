# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging
import os
from enum import Enum

import boto3
from libumccr import libjson
from rest_framework import filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.viewsets import ViewSet
from rest_framework import parsers

logger = logging.getLogger(__name__)

from data_processors.pipeline.domain.manops import RNAsumReport


class ManOpsViewSet(ViewSet):
    parser_classes = [parsers.JSONParser]

    def list(self, request):
        """
        You have hit GET /manops. This is placeholder only list endpoint. Nothing here yet.
        Please use POST /manops endpoint and path with appropriate payload to run manual operations.
        """
        logger.debug(self.request.query_params)
        return Response(data={
            'message': self.list.__doc__
        }, status=status.HTTP_400_BAD_REQUEST)

    def create(self, request):
        """
        Need more specific path, POST /manops endpoint does not exist.
        Please hit with the correct path with appropriate payload to run the /manops endpoint.
        """
        return Response(data={
            'message': self.create.__doc__
        }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=['post'])
    def run_rnasum(self, request):
        """
        Expected payload:
        {
            "wfr_id": "wfr.<umccrise_wfr_id>",
            "subject_id": "SBJ00000",
            "dataset": "BRCA"  See: https://github.com/umccr/RNAsum/blob/master/TCGA_projects_summary.md#pan-cancer-dataset
        }
        wfr_id takes precedence over subject_id
        """

        payload = request.data
        wfr_id = payload.get("wfr_id")
        subject_id = payload.get("subject_id")
        dataset = payload.get("dataset")

        # Validate payload
        if not dataset or not (subject_id or wfr_id):
            return Response(data={
                'message': "Invalid payload. Payload required: dataset, wfr_id or subject_id "
            }, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Create domain
            rnasum_report = RNAsumReport()
            rnasum_report.add_dataset(dataset)
            if wfr_id:
                rnasum_report.add_workflow(wfr_id=wfr_id)
            else:
                logger.info('Find latest wfr_id from subject_id latest UMCCRISE')
                rnasum_report.add_workflow_from_subject(subject_id=subject_id)

            # Triggering and response
            report_response = rnasum_report.generate()
            lambda_details = {
                'payload': payload,
                'status': report_response['StatusCode'],
                'response': libjson.dumps(report_response),
            }
            logger.info(libjson.dumps(lambda_details))
            return Response(data=lambda_details, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(e)
            return Response(data={'message': "INTERNAL_SERVER_ERROR"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
