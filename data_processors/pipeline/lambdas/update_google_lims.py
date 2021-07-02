try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

from datetime import datetime
from typing import List

from data_portal.models import Workflow, LIMSRow, LabMetadata, SequenceRun
from data_processors import const
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.tools import liborca, libregex
from utils import libssm, libgdrive


def get_libs_from_run(workflow: Workflow) -> List[dict]:
    """
    Example result structure is list of dicts like:
    {
        "id": "L2100001",
        "lane": "2",
        "run_id": "r.aiursfhpwugrpghur",
        "run_name": "210101_A00130_0100_BH2N5WDMXX"
    }
    :param workflow: the Workflow object
    :return:
    """
    run_name = workflow.sequence_run.name
    run_id = workflow.sequence_run.run_id

    # get BCL Convert workflow output (which contains FastqListRow records)
    fastq_list_output = liborca.parse_bcl_convert_output(workflow.output)

    lib_records: List[dict] = list()
    for fqlr in fastq_list_output:
        lane = fqlr['lane']
        library_id = libregex.SAMPLE_REGEX_OBJS['unique_id'].fullmatch(fqlr['rgsm']).group(2)
        lib_records.append({
            "id": library_id,
            "lane": lane,
            "run_id": run_id,
            "run_name": run_name
        })

    return lib_records


def get_run_number_from_run_name(run_name: str) -> int:
    return int(run_name.split('_')[2])


def get_timestamp_from_run_name(run_name: str) -> str:
    date_part = run_name.split('_')[0]
    # convert to format YYYY-MM-DD
    return datetime.strptime(date_part, '%y%m%d').strftime('%Y-%m-%d')


def create_lims_entry(lib_id: str, seq_run_name: str) -> LIMSRow:
    # convert library ID + lane + run ID into a Google LIMS record
    # TODO: deal with no/multiple return values
    lab_meta: LabMetadata = LabMetadata.objects.get(library_id=lib_id)

    lims_row = LIMSRow(
        illumina_id=seq_run_name,
        run=get_run_number_from_run_name(seq_run_name),
        timestamp=get_timestamp_from_run_name(seq_run_name),
        subject_id=lab_meta.subject_id,
        sample_id=lab_meta.sample_id,
        library_id=lab_meta.library_id,
        external_subject_id=lab_meta.external_subject_id,
        external_sample_id=lab_meta.external_sample_id,
        sample_name=lab_meta.sample_name,
        project_owner=lab_meta.project_owner,
        project_name=lab_meta.project_name,
        type=lab_meta.type,
        assay=lab_meta.assay,
        override_cycles=lab_meta.override_cycles,
        phenotype=lab_meta.phenotype,
        source=lab_meta.source,
        quality=lab_meta.quality,
        workflow=lab_meta.workflow
    )

    return lims_row


def convert_limsrow_to_tuple(limsrow: LIMSRow) -> tuple:
    return (
        limsrow.illumina_id,
        limsrow.run,
        limsrow.timestamp,
        limsrow.subject_id,
        limsrow.sample_id,
        limsrow.library_id,
        limsrow.external_subject_id,
        limsrow.external_sample_id,
        '-',  # ExternalLibraryID
        limsrow.sample_name,
        limsrow.project_owner,
        limsrow.project_name,
        '-',  # ProjectCustodian
        limsrow.type,
        limsrow.assay,
        limsrow.override_cycles,
        limsrow.phenotype,
        limsrow.source,
        limsrow.quality,
        '-',  # Topup
        '-',  # SecondaryAnalysis
        limsrow.workflow,
        '-',  # Tags
        '-',  # FASTQ path pattern
        '-',  # Number of FASTQs
        '-',  # Results
        '-',  # Trello
        '-',  # Notes
        '-'  # ToDo
    )


def update_google_lims_sheet(lims_rows: List[LIMSRow]):
    lims_sheet_id = libssm.get_secret(const.LIMS_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    data: List[tuple] = list()
    for row in lims_rows:
        data.append(convert_limsrow_to_tuple(row))

    resp = libgdrive.append_records(account_info=account_info, file_id=lims_sheet_id, data=data)
    return resp


def update_google_lims(workflow: Workflow):
    # Need to find all libraries (+ lane) that were part of this run
    # need to use the "_topup" version of the libraries (or include the lane to the Google LIMS)
    library_records = get_libs_from_run(workflow=workflow)

    lims_rows = list()
    for lib_rec in library_records:
        lims_row = create_lims_entry(lib_id=lib_rec['id'], seq_run_name=lib_rec['run_name'])
        lims_rows.append(lims_row)

    resp = update_google_lims_sheet(lims_rows)
    return resp


def get_workflow_for_seq_run_name(seq_run_name: str) -> Workflow:

    search_resp = Workflow.objects.filter(type_name=WorkflowType.BCL_CONVERT.value,
                                          end_status=WorkflowStatus.SUCCEEDED.value)
    if len(search_resp) < 1:
        raise ValueError(f"Could not find successful BCL Convert workflows!")

    # collect workflows with matching sequence run name
    workflows: List[Workflow] = list()
    for wf in search_resp:
        seq_run: SequenceRun = wf.sequence_run
        if seq_run.name == seq_run_name:
            workflows.append(wf)

    if len(workflows) < 1:
        raise ValueError(f"Could not find workflow for sequence run {seq_run_name}")

    if len(workflows) == 1:
        return workflows[0]

    # if there are more than one matching workflows (e.g. due to reruns) get the latest one
    latest_wf = workflows[0]  # assume the first record is the latest one
    # see if there is a newer one
    for nest_wf in workflows:
        if nest_wf.end > latest_wf.end:
            latest_wf = nest_wf
    return latest_wf


def handler(event, context):
    """
    Update the Google LIMS sheet given either an ICA workflow run ID or an Illumina Sequence run name.
    Only one of those parameters is required.
    {
        "wfr_id": "wfr.08de0d4af8894f0e95d6bc1f58adf1dc",
        "sequence_run_name": "201027_A01052_0022_BH5KDCESXY"
    }
    :param event: request payload as above with either Workflow Run ID or Sequence Run name
    :param context: Not used
    :return: dict response of the Google Sheets update request
    """
    # we want a Workflow object, which we can get either directly using a workflow ID
    # or via a sequence run lookup/mapping
    wrf_id = event.get('wfr_id')
    seq_run_name = event.get('sequence_run_name')
    if wrf_id:
        # there should be exactly one record for any 'wfr' ID
        workflow = Workflow.objects.get(wfr_id=wrf_id)
    elif seq_run_name:
        workflow = get_workflow_for_seq_run_name(seq_run_name)
    else:
        raise ValueError(f"Event does not contain expected parameters (wfr_id/sequence_run_name): {event}")

    return update_google_lims(workflow)
