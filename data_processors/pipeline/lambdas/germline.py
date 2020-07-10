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
        'fastq1': "SAMPLE_NAME_S1_R1_001.fastq.gz",
        'fastq2': "SAMPLE_NAME_S1_R2_001.fastq.gz",
        'sample_name': "SAMPLE_NAME",
        'seq_run_id': "sequence run id",
        'seq_name': "sequence run name",
    }

    :param event:
    :param context:
    :return: workflow db record id, wfr_id, sample_name in JSON string
    """

    logger.info(f"Start processing {WorkflowType.GERMLINE.name} event")
    logger.info(libjson.dumps(event))

    fastq1 = event['fastq1']
    fastq2 = event['fastq2']
    sample_name = event['sample_name']
    seq_run_id = event.get('seq_run_id', None)
    seq_name = event.get('seq_name', None)

    iap_workflow_prefix = "/iap/workflow"

    # read input template from parameter store
    input_template = libssm.get_ssm_param(f"{iap_workflow_prefix}/{WorkflowType.GERMLINE.value}/input")
    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input['fq1-dragen']['location'] = fastq1
    workflow_input['fq2-dragen']['location'] = fastq2
    workflow_input['outdir-dragen'] = f"dragenGermline-{sample_name}"
    workflow_input['rgid-dragen'] = f"{sample_name}"
    workflow_input['rgsm-dragen'] = f"{sample_name}"
    workflow_input['outprefix-dragen'] = f"{sample_name}"
    workflow_input['outputDir-mulitqc'] = f"out-dir-{sample_name}"
    workflow_input['subset-bam-name-sambamba'] = f"{sample_name}-subset.hla.bam"
    workflow_input['sample-name'] = f"{sample_name}"
    workflow_input['output-dirname'] = f"{sample_name}_HLA_calls"
    workflow_input['outPrefix-somalier'] = f"{sample_name}"
    workflow_input['outputDir-somalier'] = f"out-dir-{sample_name}"

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(f"{iap_workflow_prefix}/{WorkflowType.GERMLINE.value}/id")
    workflow_version = libssm.get_ssm_param(f"{iap_workflow_prefix}/{WorkflowType.GERMLINE.value}/version")

    sqr = services.get_sequence_run_by_run_id(seq_run_id) if seq_run_id else None

    # construct and format workflow run name convention
    # [RUN_NAME_PREFIX]__[WORKFLOW_TYPE]__[SEQUENCE_RUN_NAME]__[SEQUENCE_RUN_ID]__[UTC_TIMESTAMP]
    run_name_prefix = "umccr__automated"
    utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
    workflow_run_name = f"{run_name_prefix}__{WorkflowType.GERMLINE.value}__{seq_name}__{seq_run_id}__{utc_now_ts}"

    wfl_run = wes_handler.launch({
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
            'type': WorkflowType.GERMLINE,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
            'sequence_run': sqr,
            'sample_name': sample_name,
        }
    )

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'sample_name': sample_name,
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': workflow.start,
    }

    logger.info(libjson.dumps(result))

    return result
