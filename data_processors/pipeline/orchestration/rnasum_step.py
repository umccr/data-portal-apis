# -*- coding: utf-8 -*-
"""rnasum_step module

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from typing import List, Dict

from libumccr.aws import libssm, libsqs

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.config import SQS_RNASUM_QUEUE_ARN
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.orchestration import _reduce_and_transform_to_df, _extract_unique_subjects, \
    _extract_unique_libraries, _mint_libraries, _extract_unique_wgs_tumor_samples
from data_processors.pipeline.services import metadata_srv, workflow_srv
from data_processors.pipeline.tools import liborca

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def perform(this_workflow: Workflow):
    # prepare job list and dispatch to job queue
    job_list = prepare_rnasum_jobs(this_workflow)
    if job_list:
        libsqs.dispatch_jobs(queue_arn=libssm.get_ssm_param(SQS_RNASUM_QUEUE_ARN), job_list=job_list)
    else:
        logger.warning(f"Calling to prepare_rnasum_jobs() return empty list, no job to dispatch...")

    submitting_subjects = []
    for job in job_list:
        submitting_subjects.append(job['subject_id'])

    return {
        "submitting_subjects": submitting_subjects
    }


def prepare_rnasum_jobs(this_workflow: Workflow) -> List[Dict]:
    """
    TL;DR is if there is 1 dragen somatic workflow run and 1 dragen WTS workflow run, 
    then there will be 1 umccrise run and 1 rnasum run.

    Basically, there is 1 to 1 between umccrise workflow and rnasum workflow, given WTS
    data for the sample is also being run.

    :param this_workflow:
    :return:
    """

    # this_workflow is umccrise

    # Get the umccrise output directory location
    umccrise_directory = liborca.parse_umccrise_workflow_output_directory(this_workflow.output)

    # Get all libraryruns related to this_(umccrise)_workflow
    umccrise_libraryrun_list: List[LibraryRun] = workflow_srv.get_all_library_runs_by_workflow(this_workflow)

    # Get metadata equivalent for these umccrise_libraryrun_list
    umccrise_meta_list: List[LabMetadata] = metadata_srv.get_metadata_for_library_runs(umccrise_libraryrun_list)

    umccrise_meta_list_df = _reduce_and_transform_to_df(umccrise_meta_list)
    subjects = _extract_unique_subjects(umccrise_meta_list_df)  # a subject that run with this_(umccrise)_workflow

    if len(subjects) > 1:
        # rare! but not impossible.
        # hopefully, "somalier fingerprint" and "subject aliasing" should be solved at elsewhere upstream.
        # there should only be a single subject belong to one umccrise run.
        # skip for now as we don't support this and/or there must be major issue with sample swap or corrupted metadata!
        logger.warning(f"[SKIP] Found multiple subjects {subjects} belong to {this_workflow.type_name} workflow run "
                       f"with {this_workflow.wfr_id}")
        return []

    this_subject = subjects[0]

    # Get this_(umccrise)_workflow related WGS tumor sample ID
    wgs_tumor_samples = _extract_unique_wgs_tumor_samples(umccrise_meta_list_df)
    if len(wgs_tumor_samples) > 1:
        # rare! but not impossible.
        logger.warning(f"[SKIP] Found multiple distinct WGS tumor samples {wgs_tumor_samples} belong to {this_subject} "
                       f"{this_workflow.type_name} workflow run with {this_workflow.wfr_id}")
        return []

    this_wgs_tumor_sample = wgs_tumor_samples[0]

    # Find the correspondent wts tumor sample library metadata based on this_subject
    wts_meta_list: List[LabMetadata] = metadata_srv.get_wts_metadata_by_subject(subject_id=this_subject)

    if not wts_meta_list:
        logger.warning(f"[SKIP] There is no clinical or research grade, WTS tumor sample library metadata "
                       f"found for {this_subject}")
        return []

    wts_meta_list_df = _reduce_and_transform_to_df(wts_meta_list)
    wts_libraries = _extract_unique_libraries(wts_meta_list_df)

    # Strip _topup, _rerun and, unify it as we merged them in wts_tumor_only workflow run
    wts_libraries_minted = _mint_libraries(wts_libraries)

    if len(wts_libraries_minted) > 1:
        # somewhat rare condition!
        # if happen so, how should we handle if there are 2 (or more) WTS tumor libraries for a given Subject?
        # we will use latest WTS library output by its sequencing time
        # see https://github.com/umccr/data-portal-apis/issues/547
        try:
            this_wts_tumor_library = metadata_srv.get_most_recent_library_id_by_sequencing_time(wts_libraries_minted)
            logger.info(f"Found multiple WTS tumor libraries {wts_libraries_minted} for {this_subject}. "
                        f"Using most recent WTS tumor library base on sequencing time: {this_wts_tumor_library}")
        except ValueError as e:
            logger.warning(f"[SKIP] Found multiple WTS tumor libraries {wts_libraries_minted} belong to {this_subject}."
                           f" Automation can not determine an appropriate WTS library by sequencing time.")
            logger.warning(e)
            return []
    else:
        this_wts_tumor_library = wts_libraries_minted[0]

    # NOTE: Expect "over-fetching" wts_tumor_only (transcriptome) runs
    # At this point, we couldn't definitively infer which SequenceRun this is induced by; as we derive metadata ☝️
    # from umccrise workflow run. By design, umccrise (and somatic tumor/normal) workflow run can go across multiple
    # sequencing Runs. So, _this_ Run information is lost i.e. this_(umccrise)_workflow.sequence_run is always null.
    # Also, this_(umccrise)_workflow related LibraryRun(s) may be linked to different instrument_run_id(s).
    # Therefore, from umccrise, we can only deduce >> WGS libraries >> unique Subject >> unique WTS tumor library
    # Then, we can only have _minted_ WTS tumor sample library_id.

    # find all transcriptome wts_tumor_only workflow runs related to this_(minted)_wts_tumor_library (via LibraryRun)
    all_wts_workflow_runs_for_this_wts_tumor_library: List[Workflow] = workflow_srv \
        .get_succeeded_by_library_id_and_workflow_type(
        library_id=this_wts_tumor_library,
        workflow_type=WorkflowType.DRAGEN_WTS,
        # we cannot use Workflow.sequence_run nor, LibraryRun.instrument_run_id nor, LibraryRun.lane
        # since we cannot effectively limit db lookup scope, this has to expect "over-fetching"
        # in most cases, this is just one transcriptome wts workflow run.
    )

    if not all_wts_workflow_runs_for_this_wts_tumor_library:
        logger.warning(f"[SKIP] No transcriptome {WorkflowType.DRAGEN_WTS.value} workflow run found for WTS tumor "
                       f"sample library {this_wts_tumor_library}")
        return []

    # consistency resolution for "over-fetching" multiple transcriptome wts_tumor_only workflow runs is to
    # get the latest wts_tumor_only workflow run among all "probable" transcriptome runs i.e. latest, greatest strategy!
    this_wts_workflow: Workflow = all_wts_workflow_runs_for_this_wts_tumor_library[0]

    # Get the dragen transcriptome output directory location
    dragen_transcriptome_directory = liborca.parse_transcriptome_workflow_output_directory(this_wts_workflow.output)

    # Get the arriba output directory location
    arriba_directory = liborca.parse_arriba_workflow_output_directory(this_wts_workflow.output)

    # Get metadata for wts tumor library
    wts_tumor_meta: LabMetadata = metadata_srv.get_metadata_by_library_id(this_wts_tumor_library)

    # Get patient specific reference dataset
    tumor_dataset = lookup_tcga_dataset(meta=wts_tumor_meta)

    deduce_umccrise_result_location_from_root_dir(umccrise_directory, this_subject, this_wgs_tumor_sample)

    job = {
        "dragen_transcriptome_directory": dragen_transcriptome_directory,
        "umccrise_directory": umccrise_directory,
        "arriba_directory": arriba_directory,
        "sample_name": wts_tumor_meta.sample_id,
        "report_directory": f"{this_subject}__{this_wts_tumor_library}",
        "dataset": tumor_dataset,
        "subject_id": this_subject,
        "tumor_library_id": this_wts_tumor_library,
    }

    return [job]


def deduce_umccrise_result_location_from_root_dir(umccrise_directory: dict, subject_id: str, wgs_tumor_sample_id: str):
    """
    Deduce umccrise_directory (dict) by one additional level down i.e. sample result subdirectory that has naming
    pattern '<subject_id>__<wgs_tumor_sample_id>'

    :param umccrise_directory:
    :param subject_id:
    :param wgs_tumor_sample_id:
    """

    if "location" in umccrise_directory.keys():
        new_basename = f"{subject_id}__{wgs_tumor_sample_id}"

        # modify the location
        original_location = umccrise_directory['location']
        umccrise_directory['location'] = original_location.rstrip("/") + "/" + new_basename

        if "basename" in umccrise_directory.keys():
            umccrise_directory['basename'] = new_basename  # modify the basename

        if "nameroot" in umccrise_directory.keys():
            umccrise_directory['nameroot'] = new_basename  # modify the nameroot


def lookup_tcga_dataset(meta: LabMetadata):
    # TODO resolve https://github.com/umccr/data-portal-apis/issues/417
    #  Implement dynamic TCGA dataset lookup
    #     given a Lab metadata for WTS tumor sample
    #     query relevant patient specific reference TCGA cancer dataset annotation
    #       via REDCAP? via FHIR? TBD
    #  See https://github.com/umccr/RNAsum/blob/master/TCGA_projects_summary.md
    #     re-write this comment into appropriate function PyDoc string after implemented

    # (Lab) meta-info object has these IDs
    subject_id = meta.subject_id
    library_id = meta.library_id
    sample_id = meta.sample_id
    ext_sample_id = meta.external_sample_id
    ext_subject_id = meta.external_subject_id

    # FIXME given any of aforementioned ID, request respective RedCAP project for TCGA dataset annotation meta info
    #  If this return None then it will use the default config set in RNAsum input template

    return None
