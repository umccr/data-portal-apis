import json
import logging
import os
from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import LibraryRun, Workflow
from data_processors.pipeline.services import metadata_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_library_run_from_sequence(payload: dict):
    instr_run_id = payload.get('instrument_run_id')
    run_id = payload.get('run_id')
    gds_folder_path = payload.get('gds_folder_path')
    gds_volume_name = payload.get('gds_volume_name')
    sample_sheet_name = payload.get('sample_sheet_name')

    samplesheet_path = gds_folder_path + os.path.sep + sample_sheet_name
    samplesheet_json = liborca.get_samplesheet_to_json(gds_volume=gds_volume_name, samplesheet_path=samplesheet_path)
    samplesheet_dict = json.loads(samplesheet_json)

    library_run_list = []
    for data_row in samplesheet_dict['Data']:
        library_run = create_or_update_library_run({
            'instrument_run_id': instr_run_id,
            'run_id': run_id,
            'library_id': data_row['Sample_Name'],
            'lane': int(data_row['Lane']),
        })
        library_run_list.append(library_run)

    return library_run_list


@transaction.atomic
def create_or_update_library_run(payload: dict):
    instr_run_id = payload.get('instrument_run_id')
    run_id = payload.get('run_id')
    library_id = payload.get('library_id')
    lane = payload.get('lane')

    qs = LibraryRun.objects.filter(library_id=library_id, instrument_run_id=instr_run_id, run_id=run_id, lane=lane)

    if not qs.exists():
        msg = f"Creating new LibraryRun (instrument_run_id={instr_run_id}, library_id={library_id}, lane={lane})"
        logger.info(msg)

        library_run = LibraryRun()
        library_run.library_id = library_id
        library_run.instrument_run_id = instr_run_id
        library_run.run_id = run_id
        library_run.lane = lane

        meta = metadata_srv.get_metadata_by_library_id(library_id)
        library_run.override_cycles = meta.override_cycles

    else:
        msg = f"Updating LibraryRun (instrument_run_id={instr_run_id}, library_id={library_id}, lane={lane})"
        logger.info(msg)

        library_run = qs.get()

    # Set optional or updatable fields
    library_run.coverage_yield = payload.get('coverage_yield', None)
    library_run.qc_pass = payload.get('qc_pass', False)
    library_run.qc_status = payload.get('qc_status', None)
    library_run.valid_for_analysis = payload.get('valid_for_analysis', True)

    library_run.save()

    return library_run


@transaction.atomic
def get_all_workflows_by_library_run(library_run: LibraryRun):
    workflow_list = list()
    qs: QuerySet = library_run.workflows
    for lib_run in qs.all():
        workflow_list.append(lib_run)
    return workflow_list


@transaction.atomic
def get_library_runs(**kwargs) -> List[LibraryRun]:
    lib_runs = list()
    qs: QuerySet = LibraryRun.objects.get_by_keyword(**kwargs)
    if qs.exists():
        for lib in qs.all():
            lib_runs.append(lib)
    return lib_runs


@transaction.atomic
def get_library_run(**kwargs):
    qs: QuerySet = LibraryRun.objects.get_by_keyword(**kwargs)
    if qs.exists():
        return qs.get()
    return None


@transaction.atomic
def link_library_run_with_workflow(library_id: str, lane: int, workflow: Workflow):
    """
    very deterministic stricter linking with library_id + lane i.e. this is the best case for exact match
    workflow is still sequence-aware
    """
    seq_name = workflow.sequence_run.instrument_run_id
    seq_run_id = workflow.sequence_run.run_id

    library_run: LibraryRun = get_library_run(
        library_id=library_id,
        instrument_run_id=seq_name,
        run_id=seq_run_id,
        lane=lane,
    )

    if library_run:
        library_run.workflows.add(workflow)
        library_run.save()
        return library_run
    else:
        logger.warning(f"LibraryRun not found for {library_id}, {lane}, {seq_name}")
        return None


@transaction.atomic
def link_library_runs_with_workflow(library_id: str, workflow: Workflow):
    """
    typically library_id is in its _pure_form_ such as rglb from FastqListRow i.e. no suffixes
    workflow is still sequence-aware
    """
    seq_name = workflow.sequence_run.instrument_run_id
    seq_run_id = workflow.sequence_run.run_id

    library_run_list: List[LibraryRun] = get_library_runs(
        library_id=library_id,
        instrument_run_id=seq_name,
        run_id=seq_run_id,
    )

    for lib_run in library_run_list:
        lib_run.workflows.add(workflow)
        lib_run.save()

    if library_run_list:
        return library_run_list
    else:
        logger.warning(f"No LibraryRun records found for {library_id}, {seq_name}")
        return None


@transaction.atomic
def link_library_runs_with_x_seq_workflow(library_id_list: List[str], workflow: Workflow):
    """
    workflow can be not sequence-aware i.e. workflow that need go across multiple sequence runs
    """
    library_run_list = list()
    qs: QuerySet = LibraryRun.objects.filter(library_id__in=library_id_list)
    if qs.exists():
        for lib_run in qs.all():
            lib_run.workflows.add(workflow)
            lib_run.save()
            library_run_list.append(lib_run)
        return library_run_list
    else:
        logger.warning(f"No LibraryRun records found for {library_id_list}")
        return None
