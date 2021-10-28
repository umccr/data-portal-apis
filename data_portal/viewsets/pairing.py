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

from data_processors.pipeline.domain.pairing import TNPairing

logger = logging.getLogger()


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
