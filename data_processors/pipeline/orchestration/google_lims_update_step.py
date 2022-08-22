# -*- coding: utf-8 -*-
"""google_lims_update_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from typing import List, Set

from django.db.models import QuerySet
from libumccr import libgdrive, libregex
from libumccr.aws import libssm

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.limsrow import LIMSRow
from data_portal.models.workflow import Workflow
from data_processors import const
from data_processors.pipeline.tools import liborca


def perform(workflow: Workflow):
    libraries = get_libs_from_run(workflow=workflow)
    instrument_run_id = workflow.sequence_run.instrument_run_id

    lims_rows = list()
    for library in libraries:
        lims_row = create_lims_entry(lib_id=library, instrument_run_id=instrument_run_id)
        lims_rows.append(lims_row)

    resp = update_google_lims_sheet(lims_rows)
    return resp


def get_libs_from_run(workflow: Workflow) -> List[str]:
    """
    Return unique libraries from BCL Convert output

    NOTE: BCL Convert output FastqListRow contains lane information
    However, LIMS does not capture Lane information
    So we just simply collect unique Library ID(s)

    If Library ID use "_topup" and/or "_rerun", we collect as is e.g.
    L0000001, L0000001_topup = L0000001, L0000001_topup

    If Library use different Lane for topup then it will reduce to e.g. for (Library, Lane) tuple
    (L0000001, 1), (L0000001, 2) = L0000001

    :param workflow: the Workflow object
    :return:
    """
    # get BCL Convert workflow output (which contains FastqListRow records)
    fastq_list_output = liborca.parse_bcl_convert_output(workflow.output)

    libraries: Set[str] = set()
    for fqlr in fastq_list_output:
        # lane = fqlr['lane']
        library_id = libregex.SAMPLE_REGEX_OBJS['unique_id'].fullmatch(fqlr['rgsm']).group(2)
        libraries.add(library_id)

    return list(libraries)


def create_lims_entry(lib_id: str, instrument_run_id: str) -> LIMSRow:
    def sanitize(value):
        if isinstance(value, str):
            value = value.strip()
        return value

    sane_library_id = sanitize(lib_id)
    sane_instrument_run_id = sanitize(instrument_run_id)

    # this will return the first match if there exists multiple metadata records for given library id
    # this may be okay as meta-info should remain the same for given library id
    # if given library id metadata is not found by now then let the program crash it as this is
    # way outlier at this point in pipeline and something bad had happened
    lab_meta: LabMetadata = LabMetadata.objects.get(library_id=sane_library_id)

    qs: QuerySet = LIMSRow.objects.filter(library_id=sane_library_id, illumina_id=sane_instrument_run_id)

    if qs.exists():
        lims_row: LIMSRow = qs.get()
    else:
        lims_row = LIMSRow()
        lims_row.illumina_id = sane_instrument_run_id
        lims_row.library_id = sane_library_id

    lims_row.run = liborca.get_run_number_from_run_name(sane_instrument_run_id)
    lims_row.timestamp = liborca.get_timestamp_from_run_name(sane_instrument_run_id)
    lims_row.subject_id = sanitize(lab_meta.subject_id)
    lims_row.sample_id = sanitize(lab_meta.sample_id)
    lims_row.external_subject_id = sanitize(lab_meta.external_subject_id)
    lims_row.external_sample_id = sanitize(lab_meta.external_sample_id)
    lims_row.sample_name = sanitize(lab_meta.sample_name)
    lims_row.project_owner = sanitize(lab_meta.project_owner)
    lims_row.project_name = sanitize(lab_meta.project_name)
    lims_row.type = sanitize(lab_meta.type)
    lims_row.assay = sanitize(lab_meta.assay)
    lims_row.override_cycles = sanitize(lab_meta.override_cycles)
    lims_row.phenotype = sanitize(lab_meta.phenotype)
    lims_row.source = sanitize(lab_meta.source)
    lims_row.quality = sanitize(lab_meta.quality)
    lims_row.workflow = sanitize(lab_meta.workflow)

    lims_row.save()

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
