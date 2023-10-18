import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.base import PortalBaseModel, PortalBaseManager
from data_portal.models.batchrun import BatchRun
from data_portal.models.sequencerun import SequenceRun
from data_processors.pipeline.domain.workflow import WorkflowStatus

logger = logging.getLogger(__name__)


class WorkflowManager(PortalBaseManager):

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
            end__isnull=True,
            end_status__iexact=WorkflowStatus.RUNNING.value
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
        qs: QuerySet = super().get_queryset()

        library_id = kwargs.get('library_id', None)
        if library_id:
            qs = qs.filter(self.reduce_multi_values_qor('libraryrun__library_id', library_id))
            kwargs.pop('library_id')

        return self.get_model_fields_query(qs, **kwargs)


class Workflow(PortalBaseModel):
    # primary key - keep this `id` internal and, internal data linking purpose only
    # advertise that, not to rely on this ID; except only when context is cleared i.e. List table then Get by `id`
    # see note https://github.com/umccr/data-portal-apis/tree/dev/docs#notes
    id = models.BigAutoField(primary_key=True)

    # model baseline
    portal_run_id = models.CharField(max_length=255, unique=True)  # business logic unique key

    # delegating its values to domain logic enum, see `data_processors.pipeline.domain.workflow.WorkflowType`
    type_name = models.CharField(max_length=255)

    # secondary fields for tracking ICA v1 WES specific workflows
    wfr_id = models.CharField(max_length=255, null=True, blank=True)
    wfl_id = models.CharField(max_length=255, null=True, blank=True)
    wfv_id = models.CharField(max_length=255, null=True, blank=True)
    version = models.CharField(max_length=255, null=True, blank=True)

    # mandatory fields at Workflow initialisation
    input = models.TextField()
    start = models.DateTimeField()

    # secondary optional fields, once Workflow has been initialised
    wfr_name = models.TextField(null=True, blank=True)
    output = models.TextField(null=True, blank=True)
    end = models.DateTimeField(null=True, blank=True)
    end_status = models.CharField(max_length=255, null=True, blank=True)
    notified = models.BooleanField(null=True, blank=True)

    # optional FK fields to link them other upstream or peer models
    sequence_run = models.ForeignKey(SequenceRun, on_delete=models.SET_NULL, null=True, blank=True)
    batch_run = models.ForeignKey(BatchRun, on_delete=models.SET_NULL, null=True, blank=True)

    objects = WorkflowManager()

    def __str__(self):
        return f"PORTAL_RUN_ID: {self.portal_run_id}, WORKFLOW_TYPE: {self.type_name}, WORKFLOW_START: {self.start}"
