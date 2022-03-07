import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.base import PortalBaseModel, PortalBaseManager

logger = logging.getLogger(__name__)


class SequenceStatus(models.TextChoices):
    STARTED = "started"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    ABORTED = "aborted"

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
        elif value in ["stopped"]:
            return cls.ABORTED
        else:
            raise ValueError(f"No matching SequenceStatus found for value: {value}")


class SequenceManager(PortalBaseManager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()
        return self.get_model_fields_query(qs, **kwargs)


class Sequence(PortalBaseModel):
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
