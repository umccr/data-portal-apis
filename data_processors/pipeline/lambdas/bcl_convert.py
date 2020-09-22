try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import copy
import logging
from datetime import datetime, timezone

from data_portal.models import Workflow
from data_processors.pipeline import services
from data_processors.pipeline.constant import WorkflowType
from data_processors.pipeline.lambdas import wes_handler
from utils import libjson, libssm

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context) -> dict:
    """event payload dict
    {
        'gds_volume_name': "bssh.xxxx",
        'gds_folder_path': "/Runs/cccc.gggg",
        'seq_run_id': "yyy",
        'seq_name': "zzz",
    }

    :param event:
    :param context:
    :return: workflow db record id and wfr_id in JSON string
    """

    logger.info(f"Start processing {WorkflowType.BCL_CONVERT.name} event")
    logger.info(libjson.dumps(event))

    run_folder = f"gds://{event['gds_volume_name']}{event['gds_folder_path']}"
    seq_run_id = event.get('seq_run_id', None)
    seq_name = event.get('seq_name', None)

    iap_workflow_prefix = "/iap/workflow"

    # read input template from parameter store
    input_template = libssm.get_ssm_param(f"{iap_workflow_prefix}/{WorkflowType.BCL_CONVERT.value}/input")
    sample_sheet_gds_path = f"{run_folder}/SampleSheet.csv"

    # TODO: call demux_metadata lambda (code) to retrieve metadata?
    metadata_event = {
        'gdsVolume': event['gds_volume_name'],
        'gdsBasePath': event['gds_folder_path'],
        'gdsSamplesheet': 'SampleSheet.csv'
    }

    samples, cycles = demux_metadata.lambda_handler(metadata_event, None)


    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input['samplesheet-split']['location'] = sample_sheet_gds_path
    workflow_input['bcl-inDir']['location'] = run_folder

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(f"{iap_workflow_prefix}/{WorkflowType.BCL_CONVERT.value}/id")
    workflow_version = libssm.get_ssm_param(f"{iap_workflow_prefix}/{WorkflowType.BCL_CONVERT.value}/version")

    sqr = services.get_sequence_run_by_run_id(seq_run_id) if seq_run_id else None

    # construct and format workflow run name convention
    # [RUN_NAME_PREFIX]__[WORKFLOW_TYPE]__[SEQUENCE_NAME]__[SEQUENCE_RUN_ID]__[UTC_TIMESTAMP]
    run_name_prefix = "umccr__automated"
    utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
    workflow_run_name = f"{run_name_prefix}__{WorkflowType.BCL_CONVERT.value}__{seq_name}__{seq_run_id}__{utc_now_ts}"

    wfl_run: dict = wes_handler.launch({
        'workflow_id': workflow_id,
        'workflow_version': workflow_version,
        'workflow_run_name': workflow_run_name,
        'workflow_input': workflow_input,
    }, context)

    workflow: Workflow = services.create_or_update_workflow(
        {
            'wfr_name': workflow_run_name,
            'wfl_id': workflow_id,
            'wfr_id': wfl_run['id'],
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.BCL_CONVERT,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
            'sequence_run': sqr,
        }
    )

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': workflow.start,
    }

    logger.info(libjson.dumps(result))

    return result
