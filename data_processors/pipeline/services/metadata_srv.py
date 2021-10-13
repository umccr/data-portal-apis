# -*- coding: utf-8 -*-
"""metadata_srv module

This service module contains _stateful_ impls, backed by Portal's LabMetadata model as center of this module aggregate.
It would be best, if this module self-contain (cohesive) to its root model, i.e. LabMetadata.
However, sometime this is not the case. So it is ok to import in (cross talk) to other model either through their
respective services and/or use them here, e.g. Workflow and/or workflow_srv.

Rules-of-thumb: every module "import" is coupling dependencies, so less import is better.
If impl is _stateless_ then they should better be in liborca module and follow guide in its module doc string.
"""
import logging
from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import Workflow, LabMetadata, LabMetadataPhenotype, LabMetadataType, LabMetadataWorkflow, \
    LibraryRun
from data_processors.pipeline.services import workflow_srv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def get_metadata_by_library_id(library_id):
    """Return exact 1 match entry by library_id from Lab Metadata table. None otherwise."""
    try:
        meta: LabMetadata = LabMetadata.objects.get(library_id__iexact=library_id)
        return meta
    except LabMetadata.DoesNotExist as err:
        logger.error(f"LabMetadata query for library_id {library_id} did not find any data! {err}")
        return None
    except LabMetadata.MultipleObjectsReturned as err:
        logger.error(f"LabMetadata query for library_id {library_id} found multiple entries! {err}")
        return None


@transaction.atomic
def filter_metadata_by_library_id(library_id):
    """Return matching entry(-ies) by library_id from Lab Metadata table. Empty list otherwise."""
    meta_list = list()
    qs: QuerySet = LabMetadata.objects.filter(library_id__icontains=library_id)
    if qs.exists():
        for meta in qs.all():
            meta_list.append(meta)
    return meta_list


@transaction.atomic
def get_metadata_by_sample_library_name_as_in_samplesheet(sample_library_name):
    """
    Return matching entry(-ies) by sample_name_as_in_samplesheet i.e., sample_name as in sample_id_library_id
    Library ID also is in its absolute form i.e. with _top(N)/_rerun(N) suffixes

    NOTE:
    this is not ideal form. we aim to improve this and we should advocate _pure_ Library ID form.
    however, we have this for the sake of legacy data support.

    :param sample_library_name:
    :return: LabMetadata otherwise None
    """
    qs: QuerySet = LabMetadata.objects.get_by_sample_library_name(sample_library_name=sample_library_name)
    if qs.exists():
        return qs.get()  # there should be exact match 1 entry only
    return None


@transaction.atomic
def get_tn_metadata_by_qc_runs(qc_workflows: List[Workflow]) -> (List[LabMetadata], List[str]):
    """
    Determine which samples are available for T/N workflow

    :param qc_workflows: Succeeded QC workflows
    :return: (List[LabMetadata], List[str])
    """
    meta_list = list()

    libraries = []
    for qc_workflow in qc_workflows:
        for lib_run in workflow_srv.get_all_library_runs_by_workflow(qc_workflow):
            libraries.append(lib_run.library_id)

    qs: QuerySet = LabMetadata.objects.filter(
        library_id__in=libraries,
        phenotype__in=[LabMetadataPhenotype.TUMOR.value, LabMetadataPhenotype.NORMAL.value],
        type__in=[LabMetadataType.WGS.value],
        workflow__in=[LabMetadataWorkflow.CLINICAL.value, LabMetadataWorkflow.RESEARCH.value],
    )

    if qs.exists():
        for meta in qs.all():
            meta_list.append(meta)

    return meta_list, libraries


@transaction.atomic
def get_subject_id_from_library_id(library_id):
    """
    Get subject from a library id through metadata objects list
    :param library_id:
    :return:
    """
    try:
        subject_id = LabMetadata.objects.get(library_id=library_id).subject_id
    except LabMetadata.DoesNotExist:
        subject_id = None
        logger.error(f"No subject found for library {library_id}")

    return subject_id


@transaction.atomic
def get_metadata_by_keywords_in(**kwargs) -> List[LabMetadata]:
    """
    Query filter by keywords IN then return list of LabMetadata object

    :param kwargs:
    :return: List[LabMetadata]
    """
    meta_list = list()
    if len(kwargs.keys()) == 0:
        return meta_list

    qs: QuerySet = LabMetadata.objects.get_by_keyword_in(**kwargs)

    if qs.exists():
        for meta in qs.all():
            meta_list.append(meta)
    return meta_list


@transaction.atomic
def get_metadata_by_keywords(**kwargs) -> List[LabMetadata]:
    """
    Query filter by keywords then return list of LabMetadata object

    :param kwargs:
    :return: List[LabMetadata]
    """
    meta_list = list()
    if len(kwargs.keys()) == 0:
        return meta_list

    qs: QuerySet = LabMetadata.objects.get_by_keyword(**kwargs)

    if qs.exists():
        for meta in qs.all():
            meta_list.append(meta)
    return meta_list


@transaction.atomic
def get_all_libraries_by_keywords(**kwargs) -> List[str]:
    """
    Query filter by keywords then collect library_id and return

    :param kwargs: See get_metadata_by_keywords()
    :return: List[str]
    """
    library_id_list = list()
    meta_list = get_metadata_by_keywords(**kwargs)
    for meta in meta_list:
        library_id_list.append(meta.library_id)
    return library_id_list


@transaction.atomic
def get_metadata_for_library_runs(library_runs: List[LibraryRun]) -> List[LabMetadata]:
    library_id_list = []
    for lib_run in library_runs:
        library_id_list.append(lib_run.library_id)
    return get_metadata_by_keywords_in(libraries=library_id_list)
