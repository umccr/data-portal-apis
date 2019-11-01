import logging
from datetime import datetime
from django.db import transaction
from django.db.models import Q, ExpressionWrapper, Value, CharField, F

from data_portal.models import S3Object, LIMSRow, S3LIMS

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@transaction.atomic
def persist_s3_object(bucket: str, key: str, last_modified_date: datetime, size: int, e_tag: str):
    """
    Persist an s3 object record into the db
    :param bucket: s3 bucket name
    :param key: s3 object key
    :param last_modified_date: s3 object last modified date
    :param size: s3 object size
    :param e_tag: s3 objec etag
    :return: number of s3 object created, number of s3-lims association records created
    """
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
        return 0, 0

    # Number of s3-lims association records we have created in this run
    new_association_count = 0

    # Find all related LIMS rows and associate them
    # Credit: https://stackoverflow.com/questions/49622088/django-filtering-queryset-by-parameter-has-part-of-fields-value
    # If the linking columns have changed, we need to modify
    key_param = ExpressionWrapper(Value(key), output_field=CharField())

    # For each attr (values), s3 object key should contain it
    attr_filter = Q()
    # AND all filters
    for attr in LIMSRow.S3_LINK_ATTRS:
        attr_filter &= Q(param__contains=F(attr))

    lims_rows = LIMSRow.objects.annotate(param=key_param).filter(attr_filter)
    lims_row: LIMSRow
    for lims_row in lims_rows:
        # Create association if not exist
        if not S3LIMS.objects.filter(s3_object=s3_object, lims_row=lims_row).exists():
            logger.info(f"Linking the S3Object ({str(s3_object)}) with LIMSRow ({str(lims_row)})")

            association = S3LIMS(s3_object=s3_object, lims_row=lims_row)
            association.save()

            new_association_count += 1

    # Check if we do find any association at all or not
    if len(lims_rows) == 0:
        logging.error(f"No association to any LIMS row is found for the S3Object (bucket={bucket}, key={key})")

    return 1, new_association_count