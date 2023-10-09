from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import S3Object


@transaction.atomic
def get_s3_files_for_path_tokens(path_tokens: list) -> List[str]:
    """
    Find S3 files in a specific portal_run_id in the path with defined string tokens

    :param path_tokens: the string tokens that have to be present in the file path
    :return: list of string representation for S3 URI
    """
    qs: QuerySet = S3Object.objects.filter(key__contains=path_tokens[0])
    for token in path_tokens[1:]:
        qs = qs.filter(key__contains=token)

    results = list()

    if qs.exists():
        for row in qs.all():
            results.append(f"s3://{row.bucket}/{row.key}")

    return results


@transaction.atomic
def get_s3_files_for_regex(pattern: str):
    """
    Find S3 files in a specific portal_run_id with defined string tokens in the key.

    :param pattern:
    :return: list of string representation for S3 URI
    """
    qs = S3Object.objects.filter(key__regex=pattern)

    results = list()

    if qs.exists():
        for row in qs.all():
            results.append(f"s3://{row.bucket}/{row.key}")

    return results
