# -*- coding: utf-8 -*-
"""star_alignment_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from typing import List

from data_portal.models import Workflow


def perform(this_workflow: Workflow):
    # todo implement
    #  0)
    #  this_workflow is an instance of succeeded bcl_convert Workflow from database
    #  we won't need to use Batcher
    #  two integration patterns/options possible:
    #    a) call upstream Lambda directly from here with Event invocation type (shortcut)
    #    b) create correspondant SQS Q, star_alignment.py processing consumer Lambda (i.e. follow existing pattern)
    #         see https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal/pipeline
    #    partially bias towards option b
    #  1)
    #  gather
    #    all WTS samples belong to this bcl_convert output
    #    query corresponding FastqListRow records of this batch by querying with rglb
    #  2)
    #  see Star Alignment payload for preparing job JSON structure
    #  https://github.com/umccr/nextflow-stack/pull/29
    #  e.g.
    payload = {
        'portal_run_id': '20230530abcdefgh',  # todo if option b, generate this as part of WorkflowHelper() at consumer Lambda side
        'subject_id': 'SBJ00001',
        'sample_id': 'PRJ230002',
        'library_id': 'L2300002',
        'fastq_fwd': 'gds://production/primary_data/230430_A00001_0001_AH1VLHDSX1/20230430qazwsxed/WTS_NebRNA/PRJ230002_L2300002_S1_L001_R1_001.fastq.gz',
        'fastq_rev': 'gds://production/primary_data/230430_A00001_0001_AH1VLHDSX1/20230430qazwsxed/WTS_NebRNA/PRJ230002_L2300002_S1_L001_R2_001.fastq.gz',
    }

    prepare_star_alignment_jobs("tbd")

    # todo finally, couple with few unittest on those functions implemented

    return {
        "tbd": "tbd"
    }


def prepare_star_alignment_jobs(tbd) -> List[dict]:
    pass
