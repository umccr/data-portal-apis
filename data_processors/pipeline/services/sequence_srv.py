import logging

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import Sequence, SequenceStatus

# from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_or_update_sequence_from_bssh_event(payload: dict):
    """NOTE payload dict is sourced from BSSH Run event"""

    run_id = payload.get('id')
    instrument_run_id = payload.get('instrumentRunId')
    sample_sheet_name = payload.get('sampleSheetName')
    gds_folder_path = payload.get('gdsFolderPath')
    gds_volume_name = payload.get('gdsVolumeName')
    reagent_barcode = payload.get('reagentBarcode')
    flowcell_barcode = payload.get('flowcellBarcode')
    date_modified = payload.get('dateModified')

    status = SequenceStatus.from_seq_run_status(payload.get('status'))

    start_time = date_modified
    end_time = None
    if status in [SequenceStatus.SUCCEEDED, SequenceStatus.FAILED]:
        end_time = date_modified

    qs: QuerySet = Sequence.objects.filter(instrument_run_id=instrument_run_id, run_id=run_id)

    if not qs.exists():
        logger.info(f"Creating new Sequence (instrument_run_id={instrument_run_id}, run_id={run_id})")

        seq = Sequence()
        seq.run_id = run_id
        seq.instrument_run_id = instrument_run_id
        seq.sample_sheet_name = sample_sheet_name
        seq.gds_folder_path = gds_folder_path
        seq.gds_volume_name = gds_volume_name
        seq.reagent_barcode = reagent_barcode
        seq.flowcell_barcode = flowcell_barcode
        seq.status = status
        seq.start_time = start_time
        seq.end_time = end_time

        # seq.sample_sheet_config = liborca.get_samplesheet_json_from_file(
        #     gds_volume=gds_volume_name,
        #     samplesheet_path=f"{gds_folder_path}/{sample_sheet_name}"
        # )
        # seq.run_config = liborca.get_run_config_from_runinfo(
        #     gds_volume=gds_volume_name,
        #     runinfo_path=f"{gds_folder_path}/RunInfo.xml"
        # )

        seq.save()
        return seq
    else:
        logger.info(f"Updating Sequence (instrument_run_id={instrument_run_id}, run_id={run_id})")

        seq: Sequence = qs.get()

        if seq.status != status.value:
            seq.status = status
            seq.end_time = end_time
            seq.save()

        return seq
