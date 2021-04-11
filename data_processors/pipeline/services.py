import logging
from datetime import datetime, timezone
from typing import List, Dict

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.utils.dateparse import parse_datetime
from django.utils.timezone import make_aware, is_aware

from data_portal.models import GDSFile, SequenceRun, Workflow, Batch, BatchRun, FastqListRow
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from utils import libslack, lookup, libjson, libdt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SLACK_SENDER_BADGE = "Portal Workflow Automation"
SLACK_FOOTER_BADGE = "Automated Workflow Event"


@transaction.atomic
def get_gds_files_for_path_tokens(volume_name: str, path_tokens: list):
    """
    Find GDS files in a specific GDS volume with defined string tokens in the path.

    :param volume_name: the GDS volume containing the files
    :param path_tokens: the string tokens that have to be present in the file path
    :return:
    """
    # TODO: check path_tokens array for minimal length
    qs = GDSFile.objects.filter(volume_name=volume_name, path__contains=path_tokens[0])
    for token in path_tokens[1:]:
        qs = qs.filter(path__contains=token)

    return qs


@transaction.atomic
def get_gds_files_for_regex(volume_name: str, pattern: str):
    """
    Find GDS files in a specific GDS volume with defined string tokens in the path.

    :param volume_name: the GDS volume containing the files
    :param pattern:
    :return:
    """
    qs = GDSFile.objects.filter(volume_name=volume_name, path__regex=pattern)

    return qs


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
    except SequenceRun.DoesNotExist as e:
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

    acl = list(filter(lambda s: s.startswith('wid'), sqr.acl))  # filter wid
    if len(acl) == 1:
        owner = lookup.get_wg_name_from_id(acl[0])
    else:
        logger.info("Multiple IDs in ACL, expected 1!")
        owner = 'undetermined'

    sender = SLACK_SENDER_BADGE
    topic = f"Notification from {sqr.msg_attr_action_type}"
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
        logger.info(
            f"{workflow.type_name} '{workflow.wfr_id}' workflow unsupported status '{workflow.end_status}'. "
            f"Not reporting to Slack!")
        return

    _topic = f"Run Name: {workflow.wfr_name}"
    _attachments = [
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
            "footer": SLACK_FOOTER_BADGE,
            "ts": libdt.get_utc_now_ts()
        }
    ]

    _resp = libslack.call_slack_webhook(SLACK_SENDER_BADGE, _topic, _attachments)

    if _resp:
        workflow.notified = True
        workflow.save()

    return _resp


@transaction.atomic
def notify_batch_run_status(batch_run_id):

    batch_run = get_batch_run(batch_run_id=batch_run_id)

    if batch_run.notified:
        logger.info(f"[SKIP] Batch Run ID [{batch_run.id}] is already notified once. Not reporting to Slack!")
        return

    _topic = f"Batch: {batch_run.batch.name}, Step: {batch_run.step.upper()}, " \
             f"Label: {batch_run.batch.id}:{batch_run.id}"

    # at the mo, BatchRun has only two states, RUNNING or not
    if batch_run.running:
        state = "running"
        slack_color = libslack.SlackColor.BLUE.value
    else:
        state = "completed"
        slack_color = libslack.SlackColor.GREEN.value

    workflows = Workflow.objects.get_by_batch_run(batch_run=batch_run)
    workflow = workflows[0]  # pick one for convenience

    cnt = 0
    stats = {
        WorkflowStatus.SUCCEEDED.value: 0,
        WorkflowStatus.FAILED.value: 0,
        WorkflowStatus.ABORTED.value: 0,
        WorkflowStatus.RUNNING.value: 0,
    }
    mtx = ""
    for wfl in workflows:
        mtx += f"{wfl.sample_name}: {str(wfl.end_status).upper()}, {wfl.wfr_id}\n"
        cnt += 1
        stats[wfl.end_status] += 1

    _title = f"Total: {cnt} " \
             f"| Running: {stats[WorkflowStatus.RUNNING.value]} " \
             f"| Succeeded: {stats[WorkflowStatus.SUCCEEDED.value]} " \
             f"| Failed: {stats[WorkflowStatus.FAILED.value]} " \
             f"| Aborted: {stats[WorkflowStatus.ABORTED.value]}"

    _attachments = [
        {
            "fallback": _topic,
            "color": slack_color,
            "pretext": f"Status: {state.upper()}, Workflow: {workflow.type_name.upper()}@{workflow.version}",
            "title": _title,
            "text": mtx,
            "footer": SLACK_FOOTER_BADGE,
            "ts": libdt.get_utc_now_ts()
        }
    ]

    _resp = libslack.call_slack_webhook(SLACK_SENDER_BADGE, _topic, _attachments)

    if _resp:
        for wfl in workflows:
            wfl.notified = True
            wfl.save()

        batch_run.notified = True
        batch_run.save()

    return _resp


def notify_outlier(topic: str, reason: str, status: str, event: dict):

    slack_color = libslack.SlackColor.GRAY.value

    sender = SLACK_SENDER_BADGE
    topic = f"Pipeline {status}: {topic}"

    fields = []
    if event:
        for k, v in event.items():
            f = {
                'title': str(k).upper(),
                'value': str(v),
                'short': True
            }
            fields.append(f)

    attachments = [
        {
            "fallback": topic,
            "color": slack_color,
            "pretext": f"Status: {status}",
            "title": f"Reason: {reason}",
            "text": "Event Attributes:",
            "fields": fields if fields else "No attributes found. Please check CloudWatch logs.",
            "footer": SLACK_FOOTER_BADGE,
            "ts": int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        }
    ]

    return libslack.call_slack_webhook(sender, topic, attachments)


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
def search_matching_runs(**kwargs):
    """
    NOTE: for more edge cases, may need to check content of input such as using JSON diff

    :param kwargs: parameters that define idempotency
    :return: list of matching workflows
    """
    matching_workflows = []
    qs = Workflow.objects.find_by_idempotent_matrix(**kwargs)
    if qs.exists():
        for workflow in qs.all():
            matching_workflows.append(workflow)
    return matching_workflows


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


@transaction.atomic
def create_or_update_fastq_list_row(fastq_list_row: dict, sequence_run: SequenceRun):
    """
    A fastq list row is a dict that contains the following struct:
    {
        'rgid': "GTGTCGGA.GCTTGCGC.1.210108_A01052_0030_ABCDEFGHIJ.MDX200237_L2100008",
        'rgsm': "MDX200237",
        'rglb': "L2100008",
        'lane': 1,
        'read_1': "gds://fastq-vol/210108_A01052_0030_ABCDEFGHIJ/Y151_I8_I8_Y151/PO/MDX200237_L2100008_S2_L001_R1_001.fastq.gz",
        'read_2': "gds://fastq-vol/210108_A01052_0030_ABCDEFGHIJ/Y151_I8_I8_Y151/PO/MDX200237_L2100008_S2_L001_R2_001.fastq.gz",
    }

    :param fastq_list_row:
    :param sequence_run:
    :return:
    """

    rgid: str = fastq_list_row['rgid']
    rgsm: str = fastq_list_row['rgsm']
    rglb: str = fastq_list_row['rglb']
    lane: int = fastq_list_row['lane']
    read_1: str = fastq_list_row['read_1']
    read_2: str = fastq_list_row['read_2']

    qs = FastqListRow.objects.filter(rgid__iexact=rgid)

    if not qs.exists():
        # create new row
        logger.info(f"Creating new FastqListRow (rgid={rgid})")
        flr = FastqListRow()
        flr.rgid = rgid
        flr.rgsm = rgsm
        flr.rglb = rglb
        flr.lane = lane
        flr.read_1 = read_1
        flr.read_2 = read_2
        flr.sequence_run = sequence_run
    else:
        # update to updatable attributes i.e.
        # Depends on business logic requirement, we may decide a particular attribute is "immutable" across different
        # runs. For now, we update everything if the same rgid is matched / already existed in db.
        logger.info(f"Updating existing FastqListRow (rgid={rgid})")
        flr = qs.get()
        flr.rgsm = rgsm
        flr.rglb = rglb
        flr.lane = lane
        flr.read_1 = read_1
        flr.read_2 = read_2
        flr.sequence_run = sequence_run

    flr.save()


@transaction.atomic
def get_fastq_list_row_by_rgid(rgid):
    try:
        fastq_list_row = FastqListRow.objects.get(rgid=rgid)
    except FastqListRow.DoesNotExist:
        return None

    return fastq_list_row
