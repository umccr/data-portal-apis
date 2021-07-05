import logging

from django.db import transaction

from data_portal.models import SequenceRun

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_or_update_sequence_run(payload: dict):
    run_id = payload.get('id')
    date_modified = payload.get('dateModified')
    status = payload.get('status')

    qs = SequenceRun.objects.filter(run_id=run_id, date_modified=date_modified, status=status)
    if not qs.exists():
        logger.info(f"Creating new SequenceRun (run_id={run_id}, date_modified={date_modified}, status={status})")
        sqr = SequenceRun()
        sqr.run_id = run_id
        sqr.date_modified = date_modified
        sqr.status = status
        sqr.gds_folder_path = payload.get('gdsFolderPath')
        sqr.gds_volume_name = payload.get('gdsVolumeName')
        sqr.reagent_barcode = payload.get('reagentBarcode')
        sqr.v1pre3_id = payload.get('v1pre3Id')
        sqr.acl = payload.get('acl')
        sqr.flowcell_barcode = payload.get('flowcellBarcode')
        sqr.sample_sheet_name = payload.get('sampleSheetName')
        sqr.api_url = payload.get('apiUrl')
        sqr.name = payload.get('name')
        sqr.instrument_run_id = payload.get('instrumentRunId')
        sqr.msg_attr_action = payload.get('messageAttributesAction')
        sqr.msg_attr_action_date = payload.get('messageAttributesActionDate')
        sqr.msg_attr_action_type = payload.get('messageAttributesActionType')
        sqr.msg_attr_produced_by = payload.get('messageAttributesProducedBy')
        sqr.save()
        return sqr
    else:
        logger.info(f"Ignore existing SequenceRun (run_id={run_id}, date_modified={date_modified}, status={status})")
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
