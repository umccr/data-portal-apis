import logging
from datetime import datetime
from typing import List

from django.db import transaction
from django.db.models import QuerySet
from django.utils.timezone import make_aware, is_aware

from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from utils import libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_or_update_workflow(model: dict):
    wfl_id = model.get('wfl_id')
    wfr_id = model.get('wfr_id')
    portal_run_id = model.get('portal_run_id')
    wfv_id = model.get('wfv_id')
    wfl_type: WorkflowType = model.get('type')

    qs = Workflow.objects.filter(wfl_id=wfl_id, wfr_id=wfr_id, wfv_id=wfv_id)

    if not qs.exists():
        logger.info(f"Creating new {wfl_type.value} workflow (wfl_id={wfl_id}, wfr_id={wfr_id}, wfv_id={wfv_id})")
        workflow = Workflow()
        workflow.wfl_id = wfl_id
        workflow.wfr_id = wfr_id
        workflow.portal_run_id = portal_run_id
        workflow.wfv_id = wfv_id
        workflow.type_name = wfl_type.value
        workflow.wfr_name = model.get('wfr_name')
        workflow.version = model.get('version')

        if model.get('sequence_run'):
            workflow.sequence_run = model.get('sequence_run')

        if model.get('batch_run'):
            workflow.batch_run = model.get('batch_run')

        _input = model.get('input')
        if _input:
            if isinstance(_input, dict):
                workflow.input = libjson.dumps(_input)  # if input is in dict
            else:
                workflow.input = _input  # expect input in raw json str

        start = model.get('start')
        if start is None:
            start = datetime.utcnow()
        if isinstance(start, datetime):
            workflow.start = start if is_aware(start) else make_aware(start)
        else:
            workflow.start = start  # expect start in raw zone-aware UTC datetime string e.g. "2020-06-25T10:45:20.438Z"

        if model.get('end_status'):
            workflow.end_status = model.get('end_status')

    else:

        logger.info(f"Updating existing {wfl_type.name} workflow (wfl_id={wfl_id}, wfr_id={wfr_id}, wfv_id={wfv_id})")
        workflow: Workflow = qs.get()

        _output = model.get('output')
        if _output:
            if isinstance(_output, dict):
                workflow.output = libjson.dumps(_output)  # if output is in dict
            else:
                workflow.output = _output  # expect output in raw json str

        if model.get('end_status'):
            workflow.end_status = model.get('end_status')

        _end = model.get('end')
        if _end:
            if isinstance(_end, datetime):
                workflow.end = _end if is_aware(_end) else make_aware(_end)
            else:
                workflow.end = _end  # expect end in raw zone-aware UTC datetime string e.g. "2020-06-25T10:45:20.438Z"

        workflow.notified = model.get('notified')

    workflow.save()

    return workflow


@transaction.atomic
def get_workflow_by_ids(wfr_id, wfv_id):
    workflow = None
    try:
        workflow = Workflow.objects.get(wfr_id=wfr_id, wfv_id=wfv_id)
    except Workflow.DoesNotExist as e:
        logger.debug(e)  # silent unless debug
    return workflow


@transaction.atomic
def get_workflows_by_wfr_ids(wfr_id_list: List[str]) -> List[Workflow]:
    workflows = list()
    qs: QuerySet = Workflow.objects.filter(wfr_id__in=wfr_id_list)
    if qs.exists():
        for w in qs.all():
            workflows.append(w)
    return workflows


@transaction.atomic
def get_running_by_sequence_run(sequence_run: SequenceRun, workflow_type: WorkflowType):
    """query for Workflows associated with this SequenceRun"""
    qs: QuerySet = Workflow.objects.get_running_by_sequence_run(
        sequence_run=sequence_run,
        type_name=workflow_type.value.lower()
    )
    return qs.all()


@transaction.atomic
def get_succeeded_by_sequence_run(sequence_run: SequenceRun, workflow_type: WorkflowType):
    """query for Succeeded Workflows associated with this SequenceRun"""
    workflows = list()

    qs: QuerySet = Workflow.objects.get_succeeded_by_sequence_run(
        sequence_run=sequence_run,
        type_name=workflow_type.value.lower()
    )

    if qs.exists():
        for w in qs.all():
            workflows.append(w)
    return workflows


@transaction.atomic
def get_workflow_for_seq_run_name(seq_run_name: str) -> Workflow:

    search_resp = Workflow.objects.filter(
        type_name=WorkflowType.BCL_CONVERT.value,
        end_status=WorkflowStatus.SUCCEEDED.value
    )

    if len(search_resp) < 1:
        raise ValueError(f"Could not find successful BCL Convert workflows!")

    # collect workflows with matching sequence run name
    workflows: List[Workflow] = list()
    for wf in search_resp:
        seq_run: SequenceRun = wf.sequence_run
        if seq_run.name == seq_run_name:
            workflows.append(wf)

    if len(workflows) < 1:
        raise ValueError(f"Could not find workflow for sequence run {seq_run_name}")

    if len(workflows) == 1:
        return workflows[0]

    # if there are more than one matching workflows (e.g. due to reruns) get the latest one
    latest_wf = workflows[0]  # assume the first record is the latest one
    # see if there is a newer one
    for nest_wf in workflows:
        if nest_wf.end > latest_wf.end:
            latest_wf = nest_wf
    return latest_wf


@transaction.atomic
def get_all_library_runs_by_workflow(workflow: Workflow):
    library_run_list = list()
    qs: QuerySet = workflow.libraryrun_set
    if qs.exists():
        for lib_run in qs.all():
            library_run_list.append(lib_run)
    return library_run_list
