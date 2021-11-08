import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.batchrun import BatchRun
from data_portal.models.sequencerun import SequenceRun
from data_processors.pipeline.domain.workflow import WorkflowStatus
from django.core.exceptions import FieldError

from .utils import filter_object_by_parameter_keyword

logger = logging.getLogger(__name__)


class WorkflowManager(models.Manager):

    def get_by_batch_run(self, batch_run: BatchRun) -> QuerySet:
        qs: QuerySet = self.filter(batch_run=batch_run)
        return qs

    def get_running_by_batch_run(self, batch_run: BatchRun) -> QuerySet:
        qs: QuerySet = self.filter(
            batch_run=batch_run,
            start__isnull=False,
            end__isnull=True,
            end_status__icontains=WorkflowStatus.RUNNING.value
        )
        return qs

    def get_completed_by_batch_run(self, batch_run: BatchRun) -> QuerySet:
        qs: QuerySet = self.filter(
            batch_run=batch_run,
            start__isnull=False,
        ).exclude(end_status__icontains=WorkflowStatus.RUNNING.value)
        return qs

    def get_running_by_sequence_run(self, sequence_run: SequenceRun, type_name: str) -> QuerySet:
        qs: QuerySet = self.filter(
            sequence_run=sequence_run,
            type_name__iexact=type_name.lower(),
            end__isnull=True
        )
        return qs

    def get_succeeded_by_sequence_run(self, sequence_run: SequenceRun, type_name: str) -> QuerySet:
        qs: QuerySet = self.filter(
            sequence_run=sequence_run,
            type_name__iexact=type_name.lower(),
            end__isnull=False,
            end_status__iexact=WorkflowStatus.SUCCEEDED.value
        )
        return qs

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        WORKFLOW_FIELD = ["library_id"]

        keywords = kwargs.get('keywords', None)
        if keywords:
            try:
                qs = filter_object_by_parameter_keyword(qs,keywords,WORKFLOW_FIELD)
            except FieldError:
                qs = self.none()

        # keyword from libraryrun model
        library_id = keywords.get('library_id', None)
        if library_id:
            qs = qs.filter(libraryrun__library_id__iexact=library_id)

        return qs


class Workflow(models.Model):
    class Meta:
        unique_together = ['wfr_id', 'wfl_id', 'wfv_id']

    id = models.BigAutoField(primary_key=True)
    wfr_name = models.TextField(null=True, blank=True)
    sample_name = models.CharField(max_length=255, null=True, blank=True)  # TODO deprecated, will be removed, see #244
    type_name = models.CharField(max_length=255)
    wfr_id = models.CharField(max_length=255)
    wfl_id = models.CharField(max_length=255)
    wfv_id = models.CharField(max_length=255)
    version = models.CharField(max_length=255)
    input = models.TextField()
    start = models.DateTimeField()
    output = models.TextField(null=True, blank=True)
    end = models.DateTimeField(null=True, blank=True)
    end_status = models.CharField(max_length=255, null=True, blank=True)
    notified = models.BooleanField(null=True, blank=True)
    sequence_run = models.ForeignKey(SequenceRun, on_delete=models.SET_NULL, null=True, blank=True)
    batch_run = models.ForeignKey(BatchRun, on_delete=models.SET_NULL, null=True, blank=True)

    objects = WorkflowManager()

    def __str__(self):
        return f"WORKFLOW_RUN_ID: {self.wfr_id}, WORKFLOW_TYPE: {self.type_name}, WORKFLOW_START: {self.start}"
