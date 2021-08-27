import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.local')
django.setup()

# ---

import json
from typing import List

from data_portal.models import LabMetadata, Workflow
from data_processors.pipeline.orchestration import tumor_normal_step
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.domain.workflow import WorkflowType


def run():
    # HOW TO:
    # RESTORE LOCAL DB FROM DB DUMP (DEV or PROD) i.e. make loaddata
    # DEPENDS ON DEV or PROD DB STATE, ADJUST TARGET WORKFLOW RUN ID
    # THEN, HIT python tn_debug.py

    last_qc_wfr_id = 'wfr.7b6e67faa611499fab9102c37ee9822a'  # from Run 166 in DEV

    # ---

    print(f"Lab metadata count: {LabMetadata.objects.count()}")

    this_workflow = Workflow.objects.get(wfr_id=last_qc_wfr_id)
    this_sqr = this_workflow.sequence_run

    succeeded: List[Workflow] = workflow_srv.get_succeeded_by_sequence_run(
        sequence_run=this_sqr,
        workflow_type=WorkflowType.DRAGEN_WGS_QC
    )

    job_list, subjects, submitting_subjects = tumor_normal_step.prepare_tumor_normal_jobs(succeeded)

    print("-"*32)
    print()
    print(f"Submitting {len(job_list)} T/N jobs for {submitting_subjects}.")
    print()
    print()
    print(json.dumps(
        {
            "subjects": subjects,
            "submitting_subjects": submitting_subjects
        }
    ))

    print()
    print()
    print()
    print(json.dumps(job_list))


if __name__ == '__main__':
    run()
