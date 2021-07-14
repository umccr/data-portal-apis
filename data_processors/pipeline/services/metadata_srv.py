import logging
from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import Workflow, LabMetadata

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
def get_metadata_by_sample_name_as_in_samplesheet(sample_name):
    """
    Return matching entry(-ies) by sample_name_as_in_samplesheet i.e., sample_name as in sample_id_library_id
    Library ID also is in its absolute form i.e. with _top(N)/_rerun(N) suffixes

    NOTE:
    this is not ideal form. we aim to improve this and we should advocate _pure_ Library ID form.
    however, we have this for the sake of legacy data support.

    :param sample_name:
    :return: LabMetadata otherwise None
    """
    qs: QuerySet = LabMetadata.objects.get_by_sample_library_name(sample_library_name=sample_name)
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


def get_library_id_from_workflow(workflow: Workflow):
    # extract library ID from DRAGEN_WGS_QC Workflow sample_name
    # TODO: is there a better way? Could use the fastq_list_row entries of the workflow 'input'

    # remove the first part (sample ID) from the sample_name to get the library ID
    # NOTE: this may not be exactly the library ID used in the DRAGEN_WGS_QC workflow (stripped off _topup/_rerun), but
    #       for our use case that does not matter, as we are merging all libs from the same subject anyway
    return '_'.join(workflow.sample_name.split('_')[1:])
