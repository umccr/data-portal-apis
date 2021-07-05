import logging
from typing import List

from django.db import transaction

from data_portal.models import Workflow, LabMetadata

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    # extract library ID from Germline WF sample_name
    # TODO: is there a better way? Could use the fastq_list_row entries of the workflow 'input'

    # remove the first part (sample ID) from the sample_name to get the library ID
    # NOTE: this may not be exactly the library ID used in the Germline workflow (stripped off _topup/_rerun), but
    #       for our use case that does not matter, as we are merging all libs from the same subject anyway
    return '_'.join(workflow.sample_name.split('_')[1:])
