try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import pandas as pd
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


def _build_error_message(reason) -> dict:
    error_message = {'message': reason}
    logger.error(libjson.dumps(error_message))
    return error_message


def validate_metadata(event, samples):
    prefix = f"Abort launching BCL Convert workflow."
    suffix = f"after lab metadata tracking sheet and sample sheet filtering step."

    # Check at least one sample is returned
    if samples is None or len(samples) == 0:
        reason = f"{prefix} No samples found {suffix}"
        services.notify_outlier(topic="No samples found", reason=reason, status="Aborted", event=event)
        return reason

    # Check each sample has "override_cycles" attribute
    has_error = False
    failed_samples = []
    for sample in samples:
        if sample.get("override_cycles", None) is None:
            failed_samples.append(sample.get("sample"))
            has_error = True
    if has_error:
        reason = f"{prefix} No Override Cycles found for samples {','.join(failed_samples)} {suffix}"
        services.notify_outlier(topic="No Override Cycles found", reason=reason, status="Aborted", event=event)
        return reason

    pass


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

    metadata: list = demux_metadata.handler({
        'gdsVolume': gds_volume_name,
        'gdsBasePath': gds_folder_path,
        'gdsSamplesheet': SampleSheetCSV.FILENAME.value
    }, None)

    metadata_df = pd.DataFrame(metadata)

    failure_reason = validate_metadata(event=event, samples=metadata)
    if failure_reason is not None:
        return _build_error_message(failure_reason)

    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input['sample_sheet']['location'] = sample_sheet_gds_path
    workflow_input['bcl_input_directory']['location'] = run_folder
    workflow_input['override_cycles_by_sample'] = [
        {
            "sample": sample.get("sample"),
            "override_cycles": sample.get("override_cycles")
        }
        for sample in metadata
    ]

    # Add in setting_by_override_cycles ->
    # for TSO500 samples this means the inclusion of the following settings
    """
    ctDNA or TSO-DNA should have the following settings
    AdapterBehavior,trim
    MinimumTrimmedReadLength,35
    MaskShortReads,35
    """
    tso_500_types = ["ctDNA", "TSO-DNA"]
    settings_by_override_cycles = []
    if not len(set(tso_500_types).intersection(metadata_df["types"].unique().tolist())) == 0:
        tso_override_cycles_list = metadata_df.query("type in @tso_500_types")["override_cycles"].unique().tolist()
        for tso_override_cycles in tso_override_cycles_list:
            # Confirm that tso_override_cycles settings to not affect other types
            types_in_override_cycles = metadata_df.query("override_cycles ==\"{}\"".format(tso_override_cycles))["types"].unique().tolist()
            other_types_with_override_cycles_setting = set(types_in_override_cycles).difference(tso_500_types)
            if not len(other_types_with_override_cycles_setting) == 0:
                # Get other samples that aren't tso500 that have the same override cycles setting
                other_samples = metadata_df.query("type not in @tso_500_types & override_cycles==\"{}\"".format(
                    tso_override_cycles))["sample"].tolist()
                # Throw warning
                logger.warning("Cannot set override cycles specifically for tso500 data, "
                               "the following samples also have same override cycles setting \"{}\": \"{}\"".format(
                                tso_override_cycles, ", ".join(other_samples)
                               ))
                # And then continue like nothing ever happened
                continue
            # Otherwise, we're clear to add in the tso500 override cycles settings
            settings_by_override_cycles.append({
                "override_cycles": tso_override_cycles,
                "settings": {
                    "adapter_behaviour": "trim",
                    "minimum_trimmed_read_length": 35,
                    "mask_short_reads": 35
                }
            })
    # Add settings by override cycles to workflow inputs
    if not len(settings_by_override_cycles) == 0:
        workflow_input["settings_by_override_cycles"] = settings_by_override_cycles

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
