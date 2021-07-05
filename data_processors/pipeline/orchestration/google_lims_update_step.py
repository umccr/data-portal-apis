# -*- coding: utf-8 -*-
"""google_lims_update_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from typing import List

from data_portal.models import Workflow, LIMSRow, LabMetadata
from data_processors import const
from data_processors.pipeline.tools import liborca, libregex
from utils import libssm, libgdrive


def perform(workflow: Workflow):
    # TODO: invoke async?
    # Need to find all libraries (+ lane) that were part of this run
    # need to use the "_topup" version of the libraries (or include the lane to the Google LIMS)
    library_records = get_libs_from_run(workflow=workflow)

    lims_rows = list()
    for lib_rec in library_records:
        lims_row = create_lims_entry(lib_id=lib_rec['id'], seq_run_name=lib_rec['run_name'])
        lims_rows.append(lims_row)

    resp = update_google_lims_sheet(lims_rows)
    return resp


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


def create_lims_entry(lib_id: str, seq_run_name: str) -> LIMSRow:
    # convert library ID + lane + run ID into a Google LIMS record
    # TODO: deal with no/multiple return values
    lab_meta: LabMetadata = LabMetadata.objects.get(library_id=lib_id)

    lims_row = LIMSRow(
        illumina_id=seq_run_name,
        run=liborca.get_run_number_from_run_name(seq_run_name),
        timestamp=liborca.get_timestamp_from_run_name(seq_run_name),
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
