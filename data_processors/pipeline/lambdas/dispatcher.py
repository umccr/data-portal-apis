try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from data_processors.pipeline.constant import FastQReadType, WorkflowType
from data_processors.pipeline.lambdas import fastq, germline
from utils import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUPPORTED_WORKFLOWS = [
    WorkflowType.GERMLINE.value,
]


def handler(event, context) -> dict:
    """event payload dict
    {
        'workflow_type': "germline",
        'gds_path': "gds://volume/path/to/fastq",
        'seq_run_id': "sequence run id",
        'seq_name': "sequence run name",
    }

    :param event:
    :param context:
    :return: list of workflows launched
    """

    workflow_type: str = event['workflow_type']

    logger.info(f"Start processing {workflow_type} workflow dispatcher event")
    logger.info(libjson.dumps(event))

    # early circuit breaker!
    if not workflow_type.lower() in SUPPORTED_WORKFLOWS:
        msg = f"Workflow type '{workflow_type}' is not yet supported for workflow dispatcher"
        logger.info(msg)
        return {
            'error': msg,
        }

    seq_run_id: str = event.get('seq_run_id')
    seq_name: str = event.get('seq_name')

    fastq_container = fastq.handler(event, context)

    fastq_map = fastq_container['fastq_map']

    list_of_workflows_launched = []
    for sample_name, bag in fastq_map.items():
        fastq_list = bag['fastq_list']

        if workflow_type.lower() == WorkflowType.GERMLINE.value.lower():
            if len(fastq_list) > FastQReadType.PAIRED_END.value:
                # pair_end only at the mo, log and skip
                logger.warning(f"SKIP SAMPLE '{sample_name}' {workflow_type} WORKFLOW LAUNCH. "
                               f"EXPECTING {FastQReadType.PAIRED_END.value} FASTQ FILES FOR "
                               f"{FastQReadType.PAIRED_END}. FOUND: {fastq_list}")
                continue

            wfr_germline = germline.handler({
                'fastq1': fastq_list[0],
                'fastq2': fastq_list[1],
                'sample_name': sample_name,
                'seq_run_id': seq_run_id,
                'seq_name': seq_name,
            }, context)

            list_of_workflows_launched.append(wfr_germline)

    result = {
        'workflow_type': workflow_type,
        'workflows': list_of_workflows_launched,
    }

    logger.info(libjson.dumps(result))

    return result
