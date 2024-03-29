import logging
from datetime import datetime, timezone
from typing import List

from django.db import transaction
from libumccr import libslack, libdt

from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.services import batch_srv, workflow_srv, metadata_srv
from data_processors.pipeline.tools import lookup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SLACK_SENDER_BADGE_AUTO = "Portal Workflow Automation"
SLACK_FOOTER_BADGE_AUTO = "ICA Pipeline: Automated Workflow Event"

SLACK_SENDER_BADGE_BSSH = "BSSH Run"
SLACK_FOOTER_BADGE_BSSH = "ICA Pipeline: BSSH.RUNS Event"


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

    sender = SLACK_SENDER_BADGE_BSSH
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
            "footer": SLACK_FOOTER_BADGE_BSSH,
            "ts": sqs_record_timestamp
        }
    ]

    return libslack.call_slack_webhook(sender, topic, attachments)


def resolve_sample_display_name(workflow: Workflow):
    workflow_type = WorkflowType.from_value(workflow.type_name)
    all_library_runs: List[LibraryRun] = workflow_srv.get_all_library_runs_by_workflow(workflow)

    if not all_library_runs:
        return None

    elif workflow_type == WorkflowType.BCL_CONVERT:
        return None

    elif workflow_type in [WorkflowType.TUMOR_NORMAL, WorkflowType.UMCCRISE, WorkflowType.RNASUM]:
        # if tumor_normal or umccrise or rnasum, we use TUMOR_SAMPLE_ID as display name
        meta_list: List[LabMetadata] = metadata_srv.get_metadata_for_library_runs(all_library_runs)
        for meta in meta_list:
            if meta.phenotype == LabMetadataPhenotype.TUMOR.value:
                return meta.sample_id

    elif workflow_type == WorkflowType.DRAGEN_WGS_QC:
        # if it is WGS QC, we use library_id + lane
        display_names = []
        for lib_run in all_library_runs:
            display_names.append(str(lib_run.library_id))
            display_names.append(str(lib_run.lane))
        return "/".join(display_names)

    else:
        # as a last resort, we use library_id
        display_names = set()
        for lib_run in all_library_runs:
            display_names.add(str(lib_run.library_id))
        return "".join(display_names) if len(display_names) == 1 else "/".join(display_names)


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

    _display_name = resolve_sample_display_name(workflow)

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
                    "title": "Sample",
                    "value": _display_name if _display_name else "Not Applicable",
                    "short": True
                },
            ],
            "footer": SLACK_FOOTER_BADGE_AUTO,
            "ts": libdt.get_utc_now_ts()
        }
    ]

    _resp = libslack.call_slack_webhook(SLACK_SENDER_BADGE_AUTO, _topic, _attachments)

    if _resp:
        workflow.notified = True
        workflow.save()

    return _resp


@transaction.atomic
def notify_batch_run_status(batch_run_id):

    batch_run = batch_srv.get_batch_run(batch_run_id=batch_run_id)

    if batch_run.notified:
        logger.info(f"[SKIP] Batch Run ID [{batch_run.id}] is already notified once. Not reporting to Slack!")
        return

    _topic = f"Batch: {batch_run.batch.name}, Step: {batch_run.step.upper()}, " \
             f"Label: {batch_run.batch.id}:{batch_run.id}"

    # at the mo, BatchRun has only two states, RUNNING or not
    if batch_run.running:
        state = "running"
    else:
        state = "completed"

    workflows = Workflow.objects.get_by_batch_run(batch_run=batch_run)
    workflow: Workflow = workflows[0]  # pick one for convenience

    _total_cnt = 0
    _stats = {
        WorkflowStatus.SUCCEEDED.value: 0,
        WorkflowStatus.FAILED.value: 0,
        WorkflowStatus.ABORTED.value: 0,
        WorkflowStatus.RUNNING.value: 0,
    }
    _metrics = ""
    for wfl in workflows:
        display_name = resolve_sample_display_name(wfl)
        _metrics += f"{display_name}: {str(wfl.end_status).upper()}, {wfl.wfr_id}\n"
        _total_cnt += 1
        _stats[wfl.end_status] += 1

    _title = f"Total: {_total_cnt} " \
             f"| Running: {_stats[WorkflowStatus.RUNNING.value]} " \
             f"| Succeeded: {_stats[WorkflowStatus.SUCCEEDED.value]} " \
             f"| Failed: {_stats[WorkflowStatus.FAILED.value]} " \
             f"| Aborted: {_stats[WorkflowStatus.ABORTED.value]}"

    if _total_cnt == _stats[WorkflowStatus.RUNNING.value]:
        # all running -> blue
        _color = libslack.SlackColor.BLUE.value

    elif _total_cnt == _stats[WorkflowStatus.SUCCEEDED.value]:
        # all succeeded -> green
        _color = libslack.SlackColor.GREEN.value

    elif _total_cnt == _stats[WorkflowStatus.FAILED.value]:
        # all failed -> red
        _color = libslack.SlackColor.RED.value

    else:
        # anything else -> orange
        _color = libslack.SlackColor.ORANGE.value

    wfl_type = str(workflow.type_name)
    if wfl_type.lower() in [WorkflowType.DRAGEN_WTS_QC.value.lower(), WorkflowType.DRAGEN_WGS_QC.value.lower()]:
        wfl_type = WorkflowType.DRAGEN_WGTS_QC.value  # override with placeholder WGTS type for a qc batch as whole

    _attachments = [
        {
            "fallback": _topic,
            "color": _color,
            "pretext": f"Status: {state.upper()}, Workflow: {wfl_type.upper()}@{workflow.version}",
            "title": _title,
            "text": _metrics,
            "footer": SLACK_FOOTER_BADGE_AUTO,
            "ts": libdt.get_utc_now_ts()
        }
    ]

    _resp = libslack.call_slack_webhook(SLACK_SENDER_BADGE_AUTO, _topic, _attachments)

    if _resp:
        for wfl in workflows:
            wfl.notified = True
            wfl.save()

        batch_run.notified = True
        batch_run.save()

    return _resp


def notify_outlier(topic: str, reason: str, status: str, event: dict):

    slack_color = libslack.SlackColor.GRAY.value

    sender = SLACK_SENDER_BADGE_AUTO
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
            "footer": SLACK_FOOTER_BADGE_AUTO,
            "ts": int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        }
    ]

    return libslack.call_slack_webhook(sender, topic, attachments)
