import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.workflow import Workflow
from django.core.exceptions import FieldError

from .utils import filter_object_by_parameter_keyword

logger = logging.getLogger(__name__)


class LibraryRunManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        OBJECT_FIELD_NAMES = self.values()[0].keys()

        keywords = kwargs.get('keywords', None)
        if keywords:
            try:
                qs = filter_object_by_parameter_keyword(qs,keywords, OBJECT_FIELD_NAMES)
            except FieldError:
                qs = self.none()

        type_name = keywords.get('type_name', None)
        if type_name:
            qs = qs.filter(workflows__type_name__iexact=type_name)

        end_status = keywords.get('end_status', None)
        if end_status:
            qs = qs.filter(workflows__end_status__iexact=end_status).distinct()

        return qs


class LibraryRun(models.Model):
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
