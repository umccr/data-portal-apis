import logging
from datetime import datetime, timezone

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.utils.dateparse import parse_datetime
from django.utils.timezone import make_aware, is_aware

from data_portal.models import GDSFile, SequenceRun, Workflow
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from utils import libslack, lookup, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def delete_gds_file(payload: dict):
    """
    Payload data structure is dict response of GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    On UMCCR portal, a unique GDS file means unique together of volume_name and path. See GDSFile model.

    :param payload:
    """
    volume_name = payload['volumeName']
    path = payload['path']

    try:
        gds_file = GDSFile.objects.get(volume_name=volume_name, path=path)
        gds_file.delete()
        logger.info(f"Deleted GDSFile: gds://{volume_name}{path}")
    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent GDSFile (volume={volume_name}, path={path}): {str(e)}")


@transaction.atomic
def create_or_update_gds_file(payload: dict):
    """
    Payload data structure is dict response of GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    On UMCCR portal, a unique GDS file means unique together of volume_name and path. See GDSFile model.

    :param payload:
    """
    volume_name = payload.get('volumeName')
    path = payload.get('path')

    qs = GDSFile.objects.filter(volume_name=volume_name, path=path)
    if not qs.exists():
        logger.info(f"Creating new GDSFile (volume_name={volume_name}, path={path})")
        gds_file = GDSFile()
    else:
        logger.info(f"Updating existing GDSFile (volume_name={volume_name}, path={path})")
        gds_file: GDSFile = qs.get()

    gds_file.file_id = payload.get('id')
    gds_file.name = payload.get('name')
    gds_file.volume_id = payload.get('volumeId')
    gds_file.volume_name = volume_name
    gds_file.type = payload.get('type', None)
    gds_file.tenant_id = payload.get('tenantId')
    gds_file.sub_tenant_id = payload.get('subTenantId')
    gds_file.path = path
    time_created = parse_datetime(payload.get('timeCreated'))
    gds_file.time_created = time_created if is_aware(time_created) else make_aware(time_created)
    gds_file.created_by = payload.get('createdBy')
    time_modified = parse_datetime(payload.get('timeModified'))
    gds_file.time_modified = time_modified if is_aware(time_modified) else make_aware(time_modified)
    gds_file.modified_by = payload.get('modifiedBy')
    gds_file.inherited_acl = payload.get('inheritedAcl', None)
    gds_file.urn = payload.get('urn')
    gds_file.size_in_bytes = payload.get('sizeInBytes')
    gds_file.is_uploaded = payload.get('isUploaded')
    gds_file.archive_status = payload.get('archiveStatus')
    time_archived = payload.get('timeArchived', None)
    if time_archived:
        time_archived = parse_datetime(time_archived)
        gds_file.time_archived = time_archived if is_aware(time_archived) else make_aware(time_archived)
    gds_file.storage_tier = payload.get('storageTier')
    gds_file.presigned_url = payload.get('presignedUrl', None)
    gds_file.save()


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
    except Workflow.DoesNotExist as e:
        logger.debug(e)  # silent unless debug
    return sequence_run


def notify_sequence_run_status(sqr: SequenceRun, sqs_record_timestamp: int, aws_account: str):

    if sqr.status == 'Uploading' or sqr.status == 'Running':
        slack_color = libslack.SlackColor.BLUE.value
    elif sqr.status == 'PendingAnalysis' or sqr.status == 'Complete':
        slack_color = libslack.SlackColor.GREEN.value
    elif sqr.status == 'FailedUpload' or sqr.status == 'Failed' or sqr.status == 'TimedOut':
        slack_color = libslack.SlackColor.RED.value
    else:
        logger.info(f"Unsupported status {sqr.status}. Not reporting to Slack!")
        return

    acl = sqr.acl
    if len(acl) == 1:
        owner = lookup.get_wg_name_from_id(acl[0])
    else:
        logger.info("Multiple IDs in ACL, expected 1!")
        owner = 'undetermined'

    sender = "Illumina Application Platform"
    topic = f"Notification from {sqr.msg_attr_action_type} (Portal)"
    attachments = [
        {
            "fallback": f"Run {sqr.instrument_run_id}: {sqr.status}",
            "color": slack_color,
            "pretext": sqr.name,
            "title": f"Run: {sqr.instrument_run_id}",
            "text": sqr.gds_folder_path,
            "fields": [
                {
                    "title": "Action",
                    "value": sqr.msg_attr_action,
                    "short": True
                },
                {
                    "title": "Action Type",
                    "value": sqr.msg_attr_action_type,
                    "short": True
                },
                {
                    "title": "Status",
                    "value": sqr.status,
                    "short": True
                },
                {
                    "title": "Volume Name",
                    "value": sqr.gds_volume_name,
                    "short": True
                },
                {
                    "title": "Action Date",
                    "value": sqr.msg_attr_action_date,
                    "short": True
                },
                {
                    "title": "Modified Date",
                    "value": sqr.date_modified,
                    "short": True
                },
                {
                    "title": "Produced By",
                    "value": sqr.msg_attr_produced_by,
                    "short": True
                },
                {
                    "title": "BSSH Run ID",
                    "value": sqr.run_id,
                    "short": True
                },
                {
                    "title": "Run Owner",
                    "value": owner,
                    "short": True
                },
                {
                    "title": "AWS Account",
                    "value": lookup.get_aws_account_name(aws_account),
                    "short": True
                }
            ],
            "footer": "IAP BSSH.RUNS Event",
            "ts": sqs_record_timestamp
        }
    ]

    return libslack.call_slack_webhook(sender, topic, attachments)


@transaction.atomic
def notify_workflow_status(workflow: Workflow):
    if not workflow.end_status:
        logger.info(f"{workflow.type_name} '{workflow.wfr_id}' workflow end status is '{workflow.end_status}'. "
                    f"Not reporting to Slack!")
        return

    if workflow.notified:
        logger.info(f"{workflow.type_name} '{workflow.wfr_id}' workflow status '{workflow.end_status}' is "
                    f"already notified once. Not reporting to Slack!")
        return

    _status: str = workflow.end_status.lower()

    if _status == WorkflowStatus.RUNNING.value.lower():
        slack_color = libslack.SlackColor.BLUE.value
    elif _status == WorkflowStatus.SUCCEEDED.value.lower():
        slack_color = libslack.SlackColor.GREEN.value
    elif _status == WorkflowStatus.FAILED.value.lower():
        slack_color = libslack.SlackColor.RED.value
    elif _status == WorkflowStatus.ABORTED.value.lower():
        slack_color = libslack.SlackColor.GRAY.value
    else:
        logger.info(f"{workflow.type_name} '{workflow.wfr_id}' workflow unsupported status '{workflow.end_status}'. "
                    f"Not reporting to Slack!")
        return

    sender = "Portal Workflow Automation"
    topic = f"Run Name: {workflow.wfr_name}"
    attachments = [
        {
            "fallback": f"RunID: {workflow.wfr_id}, Status: {_status.upper()}",
            "color": slack_color,
            "pretext": f"Status: {_status.upper()}",
            "title": f"RunID: {workflow.wfr_id}",
            "text": "Workflow Attributes:",
            "fields": [
                {
                    "title": "Workflow Type",
                    "value": workflow.type_name,
                    "short": True
                },
                {
                    "title": "Workflow ID",
                    "value": workflow.wfl_id,
                    "short": True
                },
                {
                    "title": "Workflow Version",
                    "value": workflow.version,
                    "short": True
                },
                {
                    "title": "Workflow Version ID",
                    "value": workflow.wfv_id,
                    "short": True
                },
                {
                    "title": "Start Time",
                    "value": workflow.start,
                    "short": True
                },
                {
                    "title": "End Time",
                    "value": workflow.end if workflow.end else "Not Applicable",
                    "short": True
                },
                {
                    "title": "Sequence Run",
                    "value": workflow.sequence_run.name if workflow.sequence_run else "Not Applicable",
                    "short": True
                },
                {
                    "title": "Sample Name",
                    "value": workflow.sample_name if workflow.sample_name else "Not Applicable",
                    "short": True
                },
            ],
            "footer": "Automated Workflow Event",
            "ts": int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        }
    ]

    resp = libslack.call_slack_webhook(sender, topic, attachments)

    if resp:
        workflow.notified = True
        workflow.save()

    return resp


@transaction.atomic
def create_or_update_workflow(model: dict):
    wfl_id = model.get('wfl_id')
    wfr_id = model.get('wfr_id')
    wfv_id = model.get('wfv_id')
    wfl_type: WorkflowType = model.get('type')

    qs = Workflow.objects.filter(wfl_id=wfl_id, wfr_id=wfr_id, wfv_id=wfv_id)

    if not qs.exists():
        logger.info(f"Creating new {wfl_type.name} workflow (wfl_id={wfl_id}, wfr_id={wfr_id}, wfv_id={wfv_id})")
        workflow = Workflow()
        workflow.wfl_id = wfl_id
        workflow.wfr_id = wfr_id
        workflow.wfv_id = wfv_id
        workflow.type_name = wfl_type.name
        workflow.wfr_name = model.get('wfr_name')
        workflow.version = model.get('version')

        if model.get('sample_name'):
            workflow.sample_name = model.get('sample_name')

        if model.get('sequence_run'):
            workflow.sequence_run = model.get('sequence_run')

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
