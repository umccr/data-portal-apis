import logging

from django.db import transaction
from django.db.models import ExpressionWrapper, Value, CharField, Q, F

from data_portal.models import LIMSRow, S3LIMS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic()
def associate_lims_rows_with_s3_obj(bucket: str, key: str, s3_object: str):
    # TODO remove association logic and drop S3LIMS table, related with global search overhaul
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
        logger.debug(f"No association to any LIMS row is found for the S3Object (bucket={bucket}, key={key})")

    return 1, new_association_count
