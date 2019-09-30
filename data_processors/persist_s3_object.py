import logging
from datetime import datetime
from django.db import transaction

from data_portal.models import S3Object, LIMSRow, S3LIMS

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_s3_object(bucket: str, key: str, last_modified_date: datetime, size: int, e_tag: str):
    query_set = S3Object.objects.filter(bucket=bucket, key=key)
    new = not query_set.exists()

    if new:
        logger.info(f"Creating a new S3Object (bucket={bucket}, key={key})")
        s3_object = S3Object(
            bucket=bucket,
            key=key
        )
    else:
        logger.info(f"Updating a existing S3Object (bucket={bucket}, key={key})")
        s3_object: S3Object = query_set.get()

    s3_object.last_modified_date = last_modified_date
    s3_object.size = size
    s3_object.e_tag = e_tag
    s3_object.save()

    if not new:
        return

    # Find all related LIMS rows and associate them
    lims_rows = LIMSRow.objects.filter(sample_id__in=key, subject_id__in=key)
    lims_row: LIMSRow
    for lims_row in lims_rows:
        # Create association if not exist
        if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
            logger.info(f"Linking the S3Object (bucket={bucket}, key={key}) with LIMSRow ({str(lims_row)})")

            association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
            association.save()

    # Check if we do find any association at all or not
    if len(lims_rows) == 0:
        logging.error(f"No association to any LIMS row is found for the S3Object (bucket={bucket}, key={key})")