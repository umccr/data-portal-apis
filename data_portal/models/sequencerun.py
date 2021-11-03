import logging

from django.db import models
from django.db.models import QuerySet

logger = logging.getLogger(__name__)


class SequenceRunManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        run_id = kwargs.get('run_id', None)
        if run_id:
            qs = qs.filter(run_id__iexact=run_id)

        name = kwargs.get('name', None)
        if name:
            qs = qs.filter(name__iexact=name)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(name__iexact=run)

        instrument_run_id = kwargs.get('instrument_run_id', None)
        if instrument_run_id:
            qs = qs.filter(instrument_run_id__iexact=instrument_run_id)

        status = kwargs.get('status', None)
        if status:
            qs = qs.filter(status__iexact=status)

        return qs


class SequenceRun(models.Model):
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
