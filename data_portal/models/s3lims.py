import logging

from django.db import models

from data_portal.models.s3object import S3Object
from data_portal.models.limsrow import LIMSRow

logger = logging.getLogger(__name__)

class S3LIMS(models.Model):
    """
    Models the association between a S3 object and a LIMS row
    """

    class Meta:
        unique_together = ['s3_object', 'lims_row']

    id = models.BigAutoField(primary_key=True)
    s3_object = models.ForeignKey(S3Object, on_delete=models.CASCADE)
    lims_row = models.ForeignKey(LIMSRow, on_delete=models.CASCADE)
