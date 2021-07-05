import logging

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.utils.dateparse import parse_datetime
from django.utils.timezone import is_aware, make_aware

from data_portal.models import GDSFile

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def get_gds_files_for_path_tokens(volume_name: str, path_tokens: list):
    """
    Find GDS files in a specific GDS volume with defined string tokens in the path.

    :param volume_name: the GDS volume containing the files
    :param path_tokens: the string tokens that have to be present in the file path
    :return:
    """
    # TODO: check path_tokens array for minimal length
    qs = GDSFile.objects.filter(volume_name=volume_name, path__contains=path_tokens[0])
    for token in path_tokens[1:]:
        qs = qs.filter(path__contains=token)

    return qs


@transaction.atomic
def get_gds_files_for_regex(volume_name: str, pattern: str):
    """
    Find GDS files in a specific GDS volume with defined string tokens in the path.

    :param volume_name: the GDS volume containing the files
    :param pattern:
    :return:
    """
    qs = GDSFile.objects.filter(volume_name=volume_name, path__regex=pattern)

    return qs


@transaction.atomic
def delete_gds_file(payload: dict):
    """
    Payload data structure is dict response of GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    On UMCCR portal, a unique GDS file means unique together of volume_name and path. See GDSFile model.

    :param payload:
    """
    volume_name = payload['volumeName']
    path = payload['path']

    try:
        gds_file = GDSFile.objects.get(volume_name=volume_name, path=path)
        gds_file.delete()
        logger.info(f"Deleted GDSFile: gds://{volume_name}{path}")
    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent GDSFile (volume={volume_name}, path={path}): {str(e)}")


@transaction.atomic
def create_or_update_gds_file(payload: dict):
    """
    Payload data structure is dict response of GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    On UMCCR portal, a unique GDS file means unique together of volume_name and path. See GDSFile model.

    :param payload:
    """
    volume_name = payload.get('volumeName')
    path = payload.get('path')

    qs = GDSFile.objects.filter(volume_name=volume_name, path=path)
    if not qs.exists():
        logger.info(f"Creating new GDSFile (volume_name={volume_name}, path={path})")
        gds_file = GDSFile()
    else:
        logger.info(f"Updating existing GDSFile (volume_name={volume_name}, path={path})")
        gds_file: GDSFile = qs.get()

    gds_file.file_id = payload.get('id')
    gds_file.name = payload.get('name')
    gds_file.volume_id = payload.get('volumeId')
    gds_file.volume_name = volume_name
    gds_file.type = payload.get('type', None)
    gds_file.tenant_id = payload.get('tenantId')
    gds_file.sub_tenant_id = payload.get('subTenantId')
    gds_file.path = path
    time_created = parse_datetime(payload.get('timeCreated'))
    gds_file.time_created = time_created if is_aware(time_created) else make_aware(time_created)
    gds_file.created_by = payload.get('createdBy')
    time_modified = parse_datetime(payload.get('timeModified'))
    gds_file.time_modified = time_modified if is_aware(time_modified) else make_aware(time_modified)
    gds_file.modified_by = payload.get('modifiedBy')
    gds_file.inherited_acl = payload.get('inheritedAcl', None)
    gds_file.urn = payload.get('urn')
    gds_file.size_in_bytes = payload.get('sizeInBytes')
    gds_file.is_uploaded = payload.get('isUploaded')
    gds_file.archive_status = payload.get('archiveStatus')
    time_archived = payload.get('timeArchived', None)
    if time_archived:
        time_archived = parse_datetime(time_archived)
        gds_file.time_archived = time_archived if is_aware(time_archived) else make_aware(time_archived)
    gds_file.storage_tier = payload.get('storageTier')
    gds_file.presigned_url = payload.get('presignedUrl', None)
    gds_file.save()
