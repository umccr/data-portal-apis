try:
    import unzip_requirements
except ImportError:
    pass

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging

from data_processors.pipeline.services import libraryrun_srv
from utils import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """event payload dict
    {
        'gds_volume_name': "bssh.xxxx",
        'gds_folder_path': "/Runs/cccc.gggg",
        'instrument_run_id': "yyy",
        'run_id': "zzz",
        'sample_sheet_name': "SampleSheet.csv",
        'runinfo_name': "RunInfo.xml",
    }

    :param event:
    :param context:
    :return: list of library id
    """
    logger.info(f"Start processing create LibraryRun from Sequence")
    logger.info(libjson.dumps(event))

    payload = {
        'gds_volume_name': event['gds_volume_name'],
        'gds_folder_path': event['gds_folder_path'],
        'instrument_run_id': event['instrument_run_id'],
        'run_id': event['run_id'],
        'sample_sheet_name': event.get('sample_sheet_name', "SampleSheet.csv"),
        'runinfo_name': event.get('runinfo_name', "RunInfo.xml"),
    }

    library_run_list = libraryrun_srv.create_library_run_from_sequence(payload)

    results = []
    for library_run in library_run_list:
        results.append(library_run.library_id)

    logger.info(libjson.dumps(results))

    return results
