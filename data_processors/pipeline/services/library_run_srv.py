import json
import logging
import os

from django.db import transaction

from data_portal.models import LibraryRun
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
