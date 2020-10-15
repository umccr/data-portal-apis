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
from data_processors.pipeline import services, constant
from data_processors.pipeline.constant import WorkflowType, SampleSheetCSV, WorkflowHelper
from data_processors.pipeline.lambdas import wes_handler, demux_metadata
from utils import libjson, libssm, libdt

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

    gds_volume_name = event['gds_volume_name']
    gds_folder_path = event['gds_folder_path']
    seq_name = event['seq_name']

    run_folder = f"gds://{gds_volume_name}{gds_folder_path}"
    seq_run_id = event.get('seq_run_id', None)

    wfl_helper = WorkflowHelper(WorkflowType.BCL_CONVERT.value)

    # read input template from parameter store
    input_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_input())
    sample_sheet_gds_path = f"{run_folder}/{SampleSheetCSV.FILENAME.value}"

    metadata: dict = demux_metadata.handler({
        'gdsVolume': gds_volume_name,
        'gdsBasePath': gds_folder_path,
        'gdsSamplesheet': SampleSheetCSV.FILENAME.value
    }, None)
    metadata_samples = metadata.get('samples', None)
    metadata_override_cycles = metadata.get('override_cycles', None)

    if metadata_samples is None or len(metadata_samples) == 0:
        reason = f"Abort launching BCL Convert workflow. " \
              f"No samples found after metadata tracking sheet and sample sheet filtering step."
        abort_message = {'message': reason}
        logger.warning(libjson.dumps(abort_message))
        services.notify_outlier(topic="No samples found", reason=reason, status="Aborted", event=event)
        return abort_message

    if metadata_override_cycles is None or len(metadata_override_cycles) == 0:
        reason = f"No Override Cycles found after metadata tracking sheet and sample sheet filtering step."
        logger.warning(libjson.dumps({'message': reason}))
        services.notify_outlier(topic="No Override Cycles found", reason=reason, status="Continue", event=event)

    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input['samplesheet']['location'] = sample_sheet_gds_path
    workflow_input['bcl-input-directory']['location'] = run_folder
    workflow_input['samples'] = metadata_samples
    workflow_input['override-cycles'] = metadata_override_cycles
    workflow_input['runfolder-name'] = seq_name

    # prepare engine_parameters
    gds_fastq_vol = libssm.get_ssm_param(constant.IAP_GDS_FASTQ_VOL)
    engine_params_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_engine_parameters())
    workflow_engine_params: dict = copy.deepcopy(libjson.loads(engine_params_template))
    workflow_engine_params['outputDirectory'] = f"gds://{gds_fastq_vol}/{seq_name}"

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
    workflow_version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())

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
        'workflow_engine_parameters': workflow_engine_params
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
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
