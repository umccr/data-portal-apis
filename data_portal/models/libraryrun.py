import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.base import PortalBaseModel, PortalBaseManager
from data_portal.models.workflow import Workflow

logger = logging.getLogger(__name__)


class LibraryRunManager(PortalBaseManager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()
        return self.get_model_fields_query(qs, **kwargs)


class LibraryRun(PortalBaseModel):
    class Meta:
        unique_together = ['library_id', 'instrument_run_id', 'run_id', 'lane']

    id = models.BigAutoField(primary_key=True)
    library_id = models.CharField(max_length=255)
    instrument_run_id = models.CharField(max_length=255)
    run_id = models.CharField(max_length=255)
    lane = models.IntegerField()
    override_cycles = models.CharField(max_length=255)

    # yield achieved with this run (to be compared against desired coverage defined in metadata)
    coverage_yield = models.CharField(max_length=255, null=True)

    # current overall QC status
    qc_pass = models.BooleanField(default=False, null=True)

    # could be progressive status from QC workflow pass to QC metric eval
    qc_status = models.CharField(max_length=255, null=True)

    # could be used for manual exclusion
    valid_for_analysis = models.BooleanField(default=True, null=True)

    workflows = models.ManyToManyField(Workflow)

    objects = LibraryRunManager()

    def __str__(self):
        return f"ID: {self.id}, LIBRARY_ID: {self.library_id}, INSTRUMENT_RUN_ID: {self.instrument_run_id}, " \
               f"RUN_ID: {self.run_id}, LANE: {self.lane}"
