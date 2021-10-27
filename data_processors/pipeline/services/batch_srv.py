import logging
from typing import List, Dict

from django.db import transaction

from data_portal.models.batch import Batch
from data_portal.models.batchrun import BatchRun
from data_portal.models.workflow import Workflow
from utils import libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def get_or_create_batch(name, created_by):
    try:
        batch = Batch.objects.get(name=name, created_by=created_by)
    except Batch.DoesNotExist:
        logger.info(f"Creating new Batch (name={name}, created_by={created_by})")
        batch = Batch()
        batch.name = name
        batch.created_by = created_by
        batch.save()

    return batch


@transaction.atomic
def update_batch(batch_id, **kwargs):
    try:
        batch = Batch.objects.get(pk=batch_id)
        context_data = kwargs.get('context_data', None)
        if isinstance(context_data, str):
            batch.context_data = context_data
        if isinstance(context_data, List) or isinstance(context_data, Dict):
            batch.context_data = libjson.dumps(context_data)
        batch.save()
        return batch
    except Batch.DoesNotExist as e:
        logger.debug(e)
    return None


@transaction.atomic
def skip_or_create_batch_run(batch: Batch, run_step: str):
    # query any on going running batch for given step
    qs = BatchRun.objects.filter(batch=batch, step=run_step, running=True)

    if not qs.exists():
        batch_run = BatchRun()
        batch_run.batch = batch
        batch_run.step = run_step
        batch_run.running = True
        batch_run.save()
        return batch_run
    else:
        logger.info(f"Batch (ID:{batch.id}, name:{batch.name}, created_by:{batch.created_by}) has existing "
                    f"running {run_step} batch. Skip creating new batch run.")
        return None


@transaction.atomic
def get_batch_run(batch_run_id):
    try:
        return BatchRun.objects.get(pk=batch_run_id)
    except BatchRun.DoesNotExist as e:
        logger.debug(e)
    return None


@transaction.atomic
def reset_batch_run(batch_run_id):
    try:
        batch_run = BatchRun.objects.get(pk=batch_run_id)
        batch_run.running = False
        batch_run.save()
        return batch_run
    except BatchRun.DoesNotExist as e:
        logger.debug(e)
    return None


@transaction.atomic
def get_batch_run_none_or_all_completed(batch_run_id):

    # load batch_run state from db at this point
    batch_run = get_batch_run(batch_run_id=batch_run_id)

    # get all workflows belong to this batch run
    total_workflows_in_batch_run = Workflow.objects.get_by_batch_run(batch_run=batch_run)

    # search all completed workflows for this batch
    completed_workflows_in_batch_run = Workflow.objects.get_completed_by_batch_run(batch_run=batch_run)

    def is_status_change():
        return total_workflows_in_batch_run.count() == completed_workflows_in_batch_run.count() \
               and batch_run.running and batch_run.notified

    if is_status_change():
        # it is completed now, reset notified to False to send notification next
        batch_run.notified = False
        # also reset running flag
        batch_run.running = False
        batch_run.save()
        return batch_run

    return None


@transaction.atomic
def get_batch_run_none_or_all_running(batch_run_id):

    # load batch_run state from db at this point
    batch_run = get_batch_run(batch_run_id=batch_run_id)

    # get all workflows belong to this batch run
    total_workflows_in_batch_run = Workflow.objects.get_by_batch_run(batch_run=batch_run)

    # search all running workflows for this batch
    running_workflows_in_batch_run = Workflow.objects.get_running_by_batch_run(batch_run=batch_run)

    if total_workflows_in_batch_run.count() == running_workflows_in_batch_run.count():
        return batch_run

    return None
