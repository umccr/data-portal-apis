import logging
from typing import List
import re

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import Workflow, LabMetadata
from data_processors.pipeline.domain.workflow import WorkflowType

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
def get_subjects_from_runs(workflows: List[Workflow]) -> list:
    subjects = set()
    for workflow in workflows:
        # use workflow helper class to extract sample/library ID from workflow
        library_id = get_library_id_from_workflow(workflow)
        logger.info(f"Extracted libraryID {library_id} from workflow {workflow} with sample name: {workflow.sample_name}")
        # use metadata helper class to get corresponding subject ID
        try:
            subject_id = LabMetadata.objects.get(library_id=library_id).subject_id
        except LabMetadata.DoesNotExist:
            subject_id = None
            logger.error(f"No subject for library {library_id}")
        if subject_id:
            subjects.add(subject_id)

    return list(subjects)


@transaction.atomic
def get_libraries_from_runs(workflows: List[Workflow]) -> list:
    libraries = set()

    for workflow in workflows:
        library_id = get_library_id_from_workflow(workflow)
        try:
            library_id_from_db = LabMetadata.objects.get(library_id=library_id).library_id
        except LabMetadata.DoesNotExist:
            logger.error(f"Library {library_id} not found in metadata, skipping")
            continue
        if library_id_from_db:
            libraries.add(library_id_from_db)

    return list(libraries)


def get_library_id_from_workflow(workflow: Workflow):
    # FIXME This may be gone for good. See https://github.com/umccr/data-portal-apis/issues/244

    # these workflows use library_id
    if workflow.type_name.lower() == WorkflowType.DRAGEN_WGS_QC.value.lower() \
            or workflow.type_name.lower() == WorkflowType.DRAGEN_TSO_CTDNA.value.lower():
        return workflow.sample_name  # << NOTE: this is library_id

    # otherwise assume legacy naming
    # remove the first part (Sample ID) from the sample_library_name to get the Library ID
    return '_'.join(workflow.sample_name.split('_')[1:])


def strip_topup_rerun_from_library_id(library_id: str) -> str:
    """
    Use some fancy regex to remove _topup or _rerun from library id
    ... well for now just some regex, pls don't try to understand it
    https://regex101.com/r/Z8IG4T/1
    :return:
    """

    library_id_regex = re.match(r"(L\d{7}|L(?:(?:PRJ|CCR|MDX|TGX)\d{6}|(?:NTC|PTC)_\w+))(?:_topup\d?|_rerun\d?)?", library_id)

    if library_id_regex is None:
        logger.warning(f"Could not get library id from {library_id}, returning input")
        return library_id

    return library_id_regex.group(1)


def sample_library_id_has_rerun(library_id: str) -> bool:
    """
    Check if a library id is a rerun
    :param library_id:
    :return:
    """
    library_id_rerun_regex = re.match(r"(?:(?:PRJ|CCR|MDX|TGX)\d{6}|(?:NTC|PTC)_\w+)(?:L\d{7}|L(?:(?:PRJ|CCR|MDX|TGX)\d{6}|(?:NTC|PTC)_\w+))(?:_topup\d?)?(_rerun\d?)?", library_id)

    if library_id_rerun_regex is None:
        logger.warning(f"Could not determine if library id has rerun - or not {library_id}, returning False")
        return False

    if library_id_rerun_regex.group(1) is not None:
        return True

    return False


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
def get_metadata_by_keywords(**kwargs) -> List[LabMetadata]:
    """
    library_id, subject_id, phenotype, type_, meta_wfl

    NOTE: type_ and meta_wfl are intentional

    :param kwargs:
    :return: List[LabMetadata]
    """
    meta_list = list()
    if len(kwargs.keys()) == 0:
        return meta_list

    qs: QuerySet = LabMetadata.objects.get_queryset()

    if "library_id" in kwargs:
        qs = qs.filter(library_id__exact=kwargs['library_id'])

    if "subject_id" in kwargs:
        qs = qs.filter(subject_id__exact=kwargs['subject_id'])

    if "phenotype" in kwargs:
        qs = qs.filter(phenotype__exact=kwargs['phenotype'])

    if "type_" in kwargs:
        qs = qs.filter(type__exact=kwargs['type_'])

    if "meta_wfl" in kwargs:
        qs = qs.filter(workflow__exact=kwargs['meta_wfl'])

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
