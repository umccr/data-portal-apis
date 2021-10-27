import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.workflow import Workflow

logger = logging.getLogger(__name__)

class LibraryRunManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        library_id = kwargs.get('library_id', None)
        if library_id:
            qs = qs.filter(library_id__exact=library_id)

        instrument_run_id = kwargs.get('instrument_run_id', None)
        if instrument_run_id:
            qs = qs.filter(instrument_run_id__iexact=instrument_run_id)

        run_id = kwargs.get('run_id', None)
        if run_id:
            qs = qs.filter(run_id__iexact=run_id)

        lane = kwargs.get('lane', None)
        if lane:
            qs = qs.filter(lane__iexact=lane)

        override_cycles = kwargs.get('override_cycles', None)
        if override_cycles:
            qs = qs.filter(override_cycles__iexact=override_cycles)

        coverage_yield = kwargs.get('coverage_yield', None)
        if coverage_yield:
            qs = qs.filter(coverage_yield__iexact=coverage_yield)

        qc_pass = kwargs.get('qc_pass', None)
        if qc_pass:
            qs = qs.filter(qc_pass__iexact=qc_pass)

        qc_status = kwargs.get('qc_status', None)
        if qc_status:
            qs = qs.filter(qc_status__iexact=qc_status)

        valid_for_analysis = kwargs.get('valid_for_analysis', None)
        if valid_for_analysis:
            qs = qs.filter(valid_for_analysis__iexact=valid_for_analysis)

        type_name = kwargs.get('type_name', None)
        if type_name:
            qs = qs.filter(workflows__type_name__iexact=type_name)

        end_status = kwargs.get('end_status', None)
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
