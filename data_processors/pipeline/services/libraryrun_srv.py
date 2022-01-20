import json
import logging
import os
from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_processors.pipeline.services import metadata_srv
from data_processors.pipeline.tools import liborca, libregex

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_library_run_from_sequence(payload: dict):
    """
    Note: Just like `data_processors.pipeline.lambdas.fastq_list_row.handler` ~ Line 115
      we strip _topup and _rerun from Library ID from SampleSheet Sample_Name column and store it.

      This is still ok as we have run info and lane along with. Hence, no lost of information.

      This is important for linking between LibraryRun and Workflow model. Because
      Workflow is working out from FastqListRow rglb that is already been stripped.

    :param payload:
    :return:
    """
    instr_run_id = payload['instrument_run_id']
    run_id = payload['run_id']
    gds_folder_path = payload['gds_folder_path']
    gds_volume_name = payload['gds_volume_name']
    sample_sheet_name = payload.get('sample_sheet_name', "SampleSheet.csv")
    runinfo_name = payload.get('runinfo_name', "RunInfo.xml")

    try:
        samplesheet_path = gds_folder_path + os.path.sep + sample_sheet_name
        samplesheet_json = liborca.get_samplesheet_to_json(gds_volume_name, samplesheet_path)
        samplesheet_dict = json.loads(samplesheet_json)
    except ValueError as e:
        logger.warning(f"SKIP populating LibraryRun for {instr_run_id}. It may be that {sample_sheet_name} is "
                       f"yet to be uploaded into the run directory.")
        return []

    library_run_list = []
    data_rows = samplesheet_dict['Data']

    no_of_lanes = None
    if 'Lane' not in data_rows[0].keys():
        # extract number of lanes from RunInfo.xml
        runinfo_path = gds_folder_path + os.path.sep + runinfo_name
        no_of_lanes = liborca.get_number_of_lanes_from_runinfo(gds_volume=gds_volume_name, runinfo_path=runinfo_path)

    for data_row in data_rows:
        library_id_as_in_samplesheet = data_row['Sample_Name']  # just working out from Sample_Name column

        rglb = liborca.strip_topup_rerun_from_library_id(library_id_as_in_samplesheet)

        # Lab metadata lookup -- we need override cycles
        meta = metadata_srv.get_metadata_by_library_id(library_id_as_in_samplesheet)

        # Strip _topup
        rglb = libregex.SAMPLE_REGEX_OBJS['topup'].split(library_id_as_in_samplesheet, 1)[0]

        # Strip _rerun
        rglb = libregex.SAMPLE_REGEX_OBJS['rerun'].split(rglb, 1)[0]

        if not data_row.get('Lane'):
            # Create a entry for each lane (samples are distributed across all lanes)
            for i in range(no_of_lanes):
                library_run = create_or_update_library_run({
                    'instrument_run_id': instr_run_id,
                    'run_id': run_id,
                    'library_id': rglb,
                    'lane': i + 1,  # convert from 0 to 1 based
                    'override_cycles': meta.override_cycles
                })
                library_run_list.append(library_run)
        else:
            library_run = create_or_update_library_run({
                'instrument_run_id': instr_run_id,
                'run_id': run_id,
                'library_id': rglb,
                'lane': int(data_row['Lane']),
                'override_cycles': meta.override_cycles
            })
            library_run_list.append(library_run)

    return library_run_list


@transaction.atomic
def create_or_update_library_run(payload: dict):
    instr_run_id = payload.get('instrument_run_id')
    run_id = payload.get('run_id')
    library_id = payload.get('library_id')
    lane = payload.get('lane')
    override_cycles = payload.get('override_cycles')

    qs = LibraryRun.objects.filter(library_id=library_id, instrument_run_id=instr_run_id, run_id=run_id, lane=lane)

    if not qs.exists():
        msg = f"Creating new LibraryRun (instrument_run_id={instr_run_id}, library_id={library_id}, lane={lane})"
        logger.info(msg)

        library_run = LibraryRun()
        library_run.library_id = library_id
        library_run.instrument_run_id = instr_run_id
        library_run.run_id = run_id
        library_run.lane = lane
        library_run.override_cycles = override_cycles

    else:
        msg = f"Updating LibraryRun (instrument_run_id={instr_run_id}, library_id={library_id}, lane={lane})"
        logger.info(msg)

        library_run = qs.get()

        # allow update override_cycles
        if override_cycles and override_cycles != library_run.override_cycles:
            library_run.override_cycles = override_cycles

    # Set optional updatable fields
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
    typically library_id is in its _pure_form_ such as rglb from FastqListRow i.e. no suffixes
    very deterministic stricter linking with library_id + lane i.e. this is the best case for exact match
    workflow is still sequence-aware
    """
    seq_name = workflow.sequence_run.instrument_run_id
    seq_run_id = workflow.sequence_run.run_id

    rglb = liborca.strip_topup_rerun_from_library_id(library_id)

    library_run: LibraryRun = get_library_run(
        library_id=rglb,
        instrument_run_id=seq_name,
        run_id=seq_run_id,
        lane=lane,
    )

    if library_run:
        library_run.workflows.add(workflow)
        library_run.save()
        return library_run
    else:
        logger.warning(f"LibraryRun not found for {rglb}, {lane}, {seq_name}")
        return None


@transaction.atomic
def link_library_runs_with_workflow(library_id: str, workflow: Workflow):
    """
    typically library_id is in its _pure_form_ such as rglb from FastqListRow i.e. no suffixes
    workflow is still sequence-aware
    """
    seq_name = workflow.sequence_run.instrument_run_id
    seq_run_id = workflow.sequence_run.run_id

    rglb = liborca.strip_topup_rerun_from_library_id(library_id)

    library_run_list: List[LibraryRun] = get_library_runs(
        library_id=rglb,
        instrument_run_id=seq_name,
        run_id=seq_run_id,
    )

    for lib_run in library_run_list:
        lib_run.workflows.add(workflow)
        lib_run.save()

    if library_run_list:
        return library_run_list
    else:
        logger.warning(f"No LibraryRun records found for {rglb}, {seq_name}")
        return None


@transaction.atomic
def link_library_runs_with_x_seq_workflow(library_id_list: List[str], workflow: Workflow):
    """
    typically library_id is in its _pure_form_ such as rglb from FastqListRow i.e. no suffixes
    workflow may be not sequence-aware i.e. workflow that need go across multiple sequence runs
    """
    library_run_list = list()

    sqr = workflow.sequence_run

    rglb_list = liborca.strip_topup_rerun_from_library_id_list(library_id_list)

    qs: QuerySet = LibraryRun.objects.filter(library_id__in=rglb_list)
    if qs.exists():
        for lib_run in qs.all():
            if sqr:
                lbr: LibraryRun = lib_run
                if sqr.instrument_run_id == lbr.instrument_run_id and sqr.run_id == lbr.run_id:
                    lbr.workflows.add(workflow)
                    lbr.save()
                    library_run_list.append(lbr)
            else:
                lib_run.workflows.add(workflow)
                lib_run.save()
                library_run_list.append(lib_run)
        return library_run_list
    else:
        logger.warning(f"No LibraryRun records found for {rglb_list}")
        return None
