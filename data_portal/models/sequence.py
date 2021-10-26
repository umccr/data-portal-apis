import logging

from django.db import models
from django.db.models import QuerySet

logger = logging.getLogger(__name__)

class SequenceStatus(models.TextChoices):
    STARTED = "started"
    FAILED = "failed"
    SUCCEEDED = "succeeded"

    @classmethod
    def from_value(cls, value):
        if value == cls.STARTED.value:
            return cls.STARTED
        elif value == cls.SUCCEEDED.value:
            return cls.SUCCEEDED
        elif value == cls.FAILED.value:
            return cls.FAILED
        else:
            raise ValueError(f"No matching SequenceStatus found for value: {value}")

    @classmethod
    def from_seq_run_status(cls, value):
        """
        See Run Status
        https://support.illumina.com/help/BaseSpace_Sequence_Hub/Source/Informatics/BS/Statuses_swBS.htm

        Note that we don't necessary support all these statuses. In the following check, those values come
        from observed values from our BSSH run events.

        See https://github.com/umccr-illumina/stratus/issues/95

        :param value:
        :return:
        """
        value = str(value).lower()
        if value in ["uploading", "running", "new"]:
            return cls.STARTED
        elif value in ["complete", "analyzing", "pendinganalysis"]:
            return cls.SUCCEEDED
        elif value in ["failed", "needsattention", "timedout", "failedupload"]:
            return cls.FAILED
        else:
            raise ValueError(f"No matching SequenceStatus found for value: {value}")


class SequenceManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        instrument_run_id = kwargs.get('instrument_run_id', None)
        if instrument_run_id:
            qs = qs.filter(instrument_run_id__iexact=instrument_run_id)

        run_id = kwargs.get('run_id', None)
        if run_id:
            qs = qs.filter(run_id__iexact=run_id)

        sample_sheet_name = kwargs.get('sample_sheet_name', None)
        if sample_sheet_name:
            qs = qs.filter(sample_sheet_name__iexact=sample_sheet_name)

        gds_folder_path = kwargs.get('gds_folder_path', None)
        if gds_folder_path:
            qs = qs.filter(gds_folder_path__iexact=gds_folder_path)

        gds_volume_name = kwargs.get('gds_volume_name', None)
        if gds_volume_name:
            qs = qs.filter(gds_volume_name__iexact=gds_volume_name)

        reagent_barcode = kwargs.get('reagent_barcode', None)
        if reagent_barcode:
            qs = qs.filter(reagent_barcode__iexact=reagent_barcode)

        flowcell_barcode = kwargs.get('flowcell_barcode', None)
        if flowcell_barcode:
            qs = qs.filter(flowcell_barcode__iexact=flowcell_barcode)

        status = kwargs.get('status', None)
        if status:
            qs = qs.filter(status__iexact=status)

        start_time = kwargs.get('start_time', None)
        if start_time:
            qs = qs.filter(start_time__iexact=start_time)

        end_time = kwargs.get('end_time', None)
        if end_time:
            qs = qs.filter(end_time__iexact=end_time)

        return qs


class Sequence(models.Model):
    class Meta:
        unique_together = ['instrument_run_id', 'run_id']

    id = models.BigAutoField(primary_key=True)
    instrument_run_id = models.CharField(max_length=255)
    run_id = models.CharField(max_length=255)
    sample_sheet_name = models.CharField(max_length=255)
    gds_folder_path = models.CharField(max_length=255)
    gds_volume_name = models.CharField(max_length=255)
    reagent_barcode = models.CharField(max_length=255)
    flowcell_barcode = models.CharField(max_length=255)
    status = models.CharField(choices=SequenceStatus.choices, max_length=255)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField(null=True, blank=True)

    # run_config = models.JSONField(null=True, blank=True)  # TODO could be it's own model
    # sample_sheet_config = models.JSONField(null=True, blank=True)  # TODO could be it's own model

    objects = SequenceManager()

    def __str__(self):
        return f"Run ID '{self.run_id}', " \
               f"Instrument ID '{self.instrument_run_id}', " \
               f"Status '{self.status}'"
