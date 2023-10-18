import logging
from datetime import datetime
from typing import List, Optional

from django.db import transaction
from django.db.models import QuerySet
from django.utils.timezone import make_aware, is_aware
from libumccr import libjson

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_or_update_workflow(model: dict):
    portal_run_id = model['portal_run_id']  # portal_run_id is mandatory
    wfl_type: WorkflowType = model['type']  # WorkflowType is mandatory

    qs = Workflow.objects.filter(portal_run_id=portal_run_id)  # `portal_run_id` is just enough unique lookup key

    if not qs.exists():
        logger.info(f"Creating new {wfl_type.value} workflow (portal_run_id={portal_run_id})")
        workflow = Workflow()

        # --- business logic: the following fields are only settable at "create" time
        # they are `immutable` after created

        workflow.portal_run_id = portal_run_id
        workflow.type_name = wfl_type.value

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

    else:

        logger.info(f"Updating existing {wfl_type.value} workflow (portal_run_id={portal_run_id})")
        workflow: Workflow = qs.get()

        # --- business logic: the following fields are only settable at "update" time
        # only mutable if the specified record has already existed

        workflow.notified = model.get('notified')

        _output = model.get('output')
        if _output:
            if isinstance(_output, dict):
                workflow.output = libjson.dumps(_output)  # if output is in dict
            else:
                workflow.output = _output  # expect output in raw json str

        _end = model.get('end')
        if _end:
            if isinstance(_end, datetime):
                workflow.end = _end if is_aware(_end) else make_aware(_end)
            else:
                workflow.end = _end  # expect end in raw zone-aware UTC datetime string e.g. "2020-06-25T10:45:20.438Z"

    # --- business logic: below are mutable fields regardless of new or existing records, if they have been payload

    if model.get('wfl_id'):
        workflow.wfl_id = model.get('wfl_id')

    if model.get('wfv_id'):
        workflow.wfv_id = model.get('wfv_id')

    if model.get('wfr_id'):
        workflow.wfr_id = model.get('wfr_id')

    if model.get('wfr_name'):
        workflow.wfr_name = model.get('wfr_name')

    if model.get('version'):
        workflow.version = model.get('version')

    if model.get('end_status'):
        workflow.end_status = model.get('end_status')

    # --- write to database
    workflow.save()

    return workflow


@transaction.atomic
def get_workflow_by_portal_run_id(portal_run_id: str):
    workflow = None
    try:
        workflow = Workflow.objects.get(portal_run_id=portal_run_id)
    except Workflow.DoesNotExist as e:
        logger.debug(e)  # silent unless debug
    return workflow


@transaction.atomic
def get_workflow_by_ids(wfr_id, wfv_id=None):
    workflow = None
    try:
        if wfv_id:
            workflow = Workflow.objects.get(wfr_id=wfr_id, wfv_id=wfv_id)
        else:
            workflow = Workflow.objects.get(wfr_id=wfr_id)
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
def get_running_by_sequence_run(sequence_run: SequenceRun, workflow_type: WorkflowType) -> List[Workflow]:
    """query for Workflows associated with this SequenceRun"""
    workflows = list()

    qs: QuerySet = Workflow.objects.get_running_by_sequence_run(
        sequence_run=sequence_run,
        type_name=workflow_type.value.lower()
    )

    if qs.exists():
        for w in qs.all():
            workflows.append(w)
    return workflows


@transaction.atomic
def get_succeeded_by_sequence_run(sequence_run: SequenceRun, workflow_type: WorkflowType) -> List[Workflow]:
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


@transaction.atomic
def get_succeeded_by_library_id_and_workflow_type(library_id: str, workflow_type: WorkflowType) -> List[Workflow]:
    """
    Get all succeeded workflow runs related to this library_id and WorkflowType. It will join call through LibraryRun
    using library_id. It is also sorted desc by workflow end time. So, latest is always at top i.e. workflow_list[0]
    """
    workflow_list = list()

    qs: QuerySet = Workflow.objects.filter(
        type_name=workflow_type.value,
        end_status=WorkflowStatus.SUCCEEDED.value,
        end__isnull=False,
        output__isnull=False,
        libraryrun__library_id=library_id,
    ).order_by('-end')

    if qs.exists():
        for wfl in qs.all():
            workflow_list.append(wfl)

    return workflow_list


@transaction.atomic
def get_labmetadata_by_wfr_id(wfr_id: str) -> List[LabMetadata]:
    """
    Get LabMetadata from given wfr_id
    """

    # Get library_id from wfr_id
    matching_library_id: QuerySet = LibraryRun.objects.values_list('library_id', named=False).filter(
        workflows__wfr_id=wfr_id).distinct()

    # find subject_id from library_id
    matching_labmetadata: QuerySet = LabMetadata.objects.filter(
        library_id__in=matching_library_id).distinct()

    return list(matching_labmetadata)


def get_labmetadata_by_workflow(workflow: Workflow) -> List[LabMetadata]:
    """
    Get LabMetadata from given workflow
    """

    # Get library_id(s) linked to this workflow
    matching_library_id: QuerySet = LibraryRun.objects.values_list('library_id', named=False).filter(
        workflows__portal_run_id=workflow.portal_run_id)

    # find metadata records for library_id(s)
    matching_labmetadata: QuerySet = LabMetadata.objects.filter(
        library_id__in=matching_library_id).distinct()

    return list(matching_labmetadata)


@transaction.atomic
def get_workflows_by_subject_id_and_workflow_type(subject_id: str,
                                                  workflow_type: WorkflowType,
                                                  workflow_status: WorkflowStatus = WorkflowStatus.SUCCEEDED,
                                                  library_ids: Optional[List[str]] = None
                                                  ) -> List[Workflow]:
    """
    Get all workflow runs related to this subject_id and WorkflowType. It will join call between LabMetadata,
    LibraryRun, and Workflow to fetch the corresponding workflows. It is also sorted desc by workflow id. So, latest is
    always at top i.e. workflow_list[0]. By default, it will query SUCCEEDED workflows unless specified otherwise
    """
    workflow_list = list()

    # Get all library_id from subject_id
    matching_library_id_qs: QuerySet = LabMetadata.objects.values_list('library_id', named=False).filter(
        subject_id=subject_id).distinct()

    if library_ids is not None:
        matching_library_id_qs = matching_library_id_qs.filter(
            library_id__in=library_ids
        )
    # Find the latest workflows from given libraryrun
    workflow_qs: QuerySet = Workflow.objects.filter(
        type_name=workflow_type.value,
        end_status=workflow_status.value,
        libraryrun__library_id__in=matching_library_id_qs,
    ).order_by('-end')

    if workflow_qs.exists():
        for wfl in workflow_qs.all():
            workflow_list.append(wfl)

    return workflow_list
