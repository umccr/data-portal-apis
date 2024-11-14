from libumccr.aws import libs3
from rest_framework.response import Response


def _error_response(message, status_code=400, err=None) -> Response:
    data = {'error': message}
    if err:
        data['detail'] = err
    return Response(
        data=data,
        status=status_code
    )


def _presign_response(bucket, key, content_disposition: str = 'inline', expires_in: int = 3600) -> Response:
    response = libs3.presign_s3_file(bucket, key, content_disposition, expires_in)
    if response[0]:
        return Response({'signed_url': response[1]})
    else:
        return Response({'error': response[1]})


def _presign_list_response(presigned_urls: list) -> Response:
    if presigned_urls and len(presigned_urls) > 0:
        return Response({'signed_urls': presigned_urls})
    else:
        return _error_response(message="No presigned URLs to return.")
    pass


def _gds_file_recs_to_presign_resps(gds_records: list) -> list:
    resps = list()
    for rec in gds_records:
        resps.append(_gds_file_rec_to_presign_resp(rec))
    return resps


def _gds_file_rec_to_presign_resp(gds_file_response) -> dict:
    return {
        'volume': gds_file_response.volume_name,
        'path': gds_file_response.path,
        'presigned_url': gds_file_response.presigned_url
    }
