import logging
from datetime import datetime
from django.db import transaction
from django.db.models import Q

from data_portal.models import S3Object, LIMSRow, S3LIMS

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_s3_object(bucket: str, key: str, last_modified_date: datetime, size: int, e_tag: str):
    query_set = S3Object.objects.filter(bucket=bucket, key=key)
    new = not query_set.exists()

    if new:
        logger.info("Creating a new S3Object (bucket=%s, key=%s)" % (bucket, key))
        s3_object = S3Object(
            bucket=bucket,
            key=key
        )
    else:
        logger.info("Updating a existing S3Object (bucket=%s, key=%s)" % (bucket, key))
        s3_object: S3Object = query_set.get()

    s3_object.last_modified_date = last_modified_date
    s3_object.size = size
    s3_object.e_tag = e_tag
    s3_object.save()

    if new:
        # Find all related LIMS rows and associate them
        lims_rows = LIMSRow.objects.filter(external_subject_id__in=key)
        lims_row: LIMSRow
        for lims_row in lims_rows:
            # Create association if not exist
            if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
                logger.info("Linking the S3Object (bucket=%s, key=%s) with LIMSRow (%s)"
                            % (bucket, key, str(lims_row)))

                association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
                association.save()

