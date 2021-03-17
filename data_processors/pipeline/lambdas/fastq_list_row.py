try:
    import unzip_requirements
except ImportError:
    pass

import django
import os
import pandas as pd
import re
from urllib.parse import urlparse

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
import re
from collections import defaultdict
from typing import List

from utils.gds import get_gds_file_list
from utils.regex_globals import SAMPLE_REGEX_OBJS

from utils import libjson

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context) -> dict:
    """event payload dict
    {
        "fastq_list_rows": [{
            "rgid": "index1.index2.lane",
            "rgsm": "sample_name",
            "rglb": "UnknownLibrary",
            "lane": int,
            "read_1": {
              "class": "File",
              "location": "gds://path/to/read_1.fastq.gz"
            },
            "read_2": {
              "class": "File",
              "location": "gds://path/to/read_2.fastq.gz"
            }
        }],
        "sequence_run_id": "YYMMDD_A0SLOT_1234_BFLOWCELLID"
    }

    Given a list of fastq list rows and a sequence run id we do the following for each item in fastq_list_rows:

    1. Check that the location attribute of read_1 and read_2 (if not null) exist on GDS.

    2. Use a regex to rename RGLB from UnknownLibrary to the library id of the sample.

    3. The same regex is used to remove the library id from RGSM, which is then just truncated to the sample name

    4. RGID is then extended from index1.index2.lane to index1.index2.lane.run_id

    5. In the event of a top up (denoted by _topup) in the sample name, RGID is extended to _topup

    The modified fastq_list_rows object is then returned

    :param event:
    :param context:
    :return: fastq container
    """

    logger.info(f"Start processing fastq list rows event")
    logger.info(libjson.dumps(event))

    fastq_list_rows: List[str] = event['fastq_list_rows']
    sequence_run_id: str = event["sequence_run_id"]

    # Easier to work this as a dataframe
    fastq_list_df = pd.DataFrame(fastq_list_rows)

    # Iterate thorugh each row and update fastq_list_row object
    new_rows = []
    for index, row in fastq_list_df.iterrows():
        # Copy out old row
        new_row = row.copy()

        # Check read_1 and read_2 exist on gds
        check_gds_file(row['read_1'])
        check_gds_file(row['read_2'])

        # Get values from library regex
        sample_match = SAMPLE_REGEX_OBJS["unique_id"].match(row["rgsm"])

        # Check sample_match isn't none (otherwise we're in trouble)  # TODO

        # Get new rgsm value
        new_row["rgsm"] = sample_match.group(1)

        # Get new rglb value
        new_row["rglb"] = sample_match.group(2)

        # Get new rgid value
        new_row["rgid"] = "{}.{}".format(row["rgid"], sequence_run_id)

        # Extend rgid value if topup
        topup_match = SAMPLE_REGEX_OBJS["topup"].match(row["rgsm"])

        # Check if we have a match to topup
        if topup_match is not None:
            new_row["rgid"] = "{}.{}".format(new_row["rgid"], "topup")


def check_gds_file(gds_path: str) -> None:
    """
    Check gds path exists, raise error if otherwise
    :param gds_path:
    """

    # Extract parts
    volume_name, path_ = parse_gds_path(gds_path)

    # Verify file exists  # TODO try - catch etc
    get_gds_file_list(volume_name=volume_name, path=path_)


def parse_gds_path(gds_path):
    gds_url_obj = urlparse(gds_path)
    return gds_url_obj.netloc, gds_url_obj.path
