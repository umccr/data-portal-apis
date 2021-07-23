import logging
from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import SequenceRun, Sequence
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_or_update_sequence_run(payload: dict):
    run_id = payload.get('id')
    instrument_run_id = payload.get('instrumentRunId')
    date_modified = payload.get('dateModified')
    status = payload.get('status')
    sample_sheet_name = payload.get('sampleSheetName')
    gds_folder_path = payload.get('gdsFolderPath')
    gds_volume_name = payload.get('gdsVolumeName')
    reagent_barcode = payload.get('reagentBarcode')
    flowcell_barcode = payload.get('flowcellBarcode')

    seq: Sequence = Sequence.objects.get(instrument_run_id=instrument_run_id, run_id=run_id)
    if not seq:
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
        seq.sample_sheet_config = liborca.get_sample_names_from_samplesheet(gds_volume=gds_volume_name,
                                                                            samplesheet_path=f"{gds_folder_path}/{sample_sheet_name}")
        seq.run_config = liborca.get_run_config_from_runinfo(gds_volume=gds_volume_name,
                                                             runinfo_path=f"{gds_folder_path}/RunInfo.xml")
        seq.save()

    qs = SequenceRun.objects.filter(run_id=run_id, date_modified=date_modified, status=status)
    if not qs.exists():
        logger.info(f"Creating new SequenceRun (run_id={run_id}, date_modified={date_modified}, status={status})")
        sqr = SequenceRun()
        sqr.instrument_run_id = instrument_run_id
        sqr.run_id = run_id
        sqr.sequence = seq
        sqr.date_modified = date_modified
        sqr.status = status
        sqr.gds_folder_path = gds_folder_path
        sqr.gds_volume_name = gds_volume_name
        sqr.reagent_barcode = reagent_barcode
        sqr.v1pre3_id = payload.get('v1pre3Id')
        sqr.acl = payload.get('acl')
        sqr.flowcell_barcode = flowcell_barcode
        sqr.sample_sheet_name = sample_sheet_name
        sqr.api_url = payload.get('apiUrl')
        sqr.name = payload.get('name')
        sqr.msg_attr_action = payload.get('messageAttributesAction')
        sqr.msg_attr_action_date = payload.get('messageAttributesActionDate')
        sqr.msg_attr_action_type = payload.get('messageAttributesActionType')
        sqr.msg_attr_produced_by = payload.get('messageAttributesProducedBy')
        sqr.save()
        return sqr
    else:
        logger.info(f"Ignore existing SequenceRun (run_id={run_id}, date_modified={date_modified}, status={status})")
        if seq.status != status:
            logger.info(f"Updating Sequence status to {status}")
            seq.status = status
            seq.save()
        return None


@transaction.atomic
def get_sequence_run_by_run_id(run_id):
    sequence_run = None
    try:
        sequence_runs = SequenceRun.objects.filter(run_id=run_id).all()
        for sqr in sequence_runs:
            if sqr.status.lower() == "PendingAnalysis".lower() or sqr.status.lower() == "Complete".lower():
                return sqr
    except SequenceRun.DoesNotExist as e:
        logger.debug(e)  # silent unless debug
    return sequence_run


@transaction.atomic
def get_sequence_run_by_instrument_run_ids(ids: List[str]) -> List[SequenceRun]:
    seq_run_list = list()

    qs: QuerySet = SequenceRun.objects.filter(instrument_run_id__in=ids)

    if qs.exists():
        for seq_run in qs.all():
            seq_run_list.append(seq_run)

    return seq_run_list
