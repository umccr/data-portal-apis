import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.base import PortalBaseModel, PortalBaseManager

logger = logging.getLogger(__name__)


class SequenceRunManager(PortalBaseManager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()
        return self.get_model_fields_query(qs, **kwargs)


class SequenceRun(PortalBaseModel):
    class Meta:
        unique_together = ['run_id', 'date_modified', 'status']

    id = models.BigAutoField(primary_key=True)
    run_id = models.CharField(max_length=255)
    date_modified = models.DateTimeField()
    status = models.CharField(max_length=255)
    gds_folder_path = models.TextField()
    gds_volume_name = models.TextField()
    reagent_barcode = models.CharField(max_length=255)
    v1pre3_id = models.CharField(max_length=255)
    acl = models.TextField()
    flowcell_barcode = models.CharField(max_length=255)
    sample_sheet_name = models.CharField(max_length=255)
    api_url = models.TextField()
    name = models.CharField(max_length=255)
    instrument_run_id = models.CharField(max_length=255)
    msg_attr_action = models.CharField(max_length=255)
    msg_attr_action_type = models.CharField(max_length=255)
    msg_attr_action_date = models.DateTimeField()
    msg_attr_produced_by = models.CharField(max_length=255)

    objects = SequenceRunManager()

    def __str__(self):
        return f"Run ID '{self.run_id}', " \
               f"Name '{self.name}', " \
               f"Instrument ID '{self.instrument_run_id}', " \
               f"Date Modified '{self.date_modified}', " \
               f"Status '{self.status}'"
