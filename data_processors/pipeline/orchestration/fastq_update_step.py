from typing import List

from data_portal.models.workflow import Workflow
from data_processors.pipeline.lambdas import fastq_list_row
from data_processors.pipeline.services import fastq_srv
from data_processors.pipeline.tools import liborca


def perform(this_workflow: Workflow):
    this_sqr = this_workflow.sequence_run

    # parse bcl convert output and get all output locations
    # build a sample info and its related fastq locations
    fastq_list_rows: List = fastq_list_row.handler({
        'fastq_list_rows': liborca.parse_bcl_convert_output(this_workflow.output),
        'seq_name': this_sqr.name,
    }, None)

    # Initialise fastq list rows object in model
    for row in fastq_list_rows:
        fastq_srv.create_or_update_fastq_list_row(row, this_sqr)

    return fastq_list_rows
