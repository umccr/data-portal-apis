# -*- coding: utf-8 -*-
"""viewsets module

NOTE:
     This is DRF based Portal API impls.
"""
import logging
from collections import defaultdict

from libica.app import gds
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet

from data_portal.viewsets.utils import _error_response, _gds_file_recs_to_presign_resps, _presign_response, \
    _presign_list_response

logger = logging.getLogger()


class PresignedUrlViewSet(ViewSet):

    def create(self, request):
        # payload is expected to be simple list of gds://... urls
        payload = self.request.data
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
        
        return _presign_response(bucket, key, expires_in=43200)
