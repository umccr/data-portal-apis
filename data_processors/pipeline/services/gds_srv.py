import logging

from django.db import transaction

from data_portal.models.gdsfile import GDSFile

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
