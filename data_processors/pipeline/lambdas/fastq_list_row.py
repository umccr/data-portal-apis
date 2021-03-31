try:
    import unzip_requirements
except ImportError:
    pass

import django
import os
import pandas as pd

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
from typing import List

from utils.gds import check_gds_file
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
        "sequencing_run": "YYMMDD_A0SLOT_1234_BFLOWCELLID"
    }

    Given a list of fastq list rows and a sequence run id we do the following for each item in fastq_list_rows:

    1. Check that the location attribute of read_1 and read_2 (if not null) exist on GDS.

    2. Use a regex to rename RGLB from UnknownLibrary to the library id of the sample.

    3. The same regex is used to remove the library id from RGSM, which is then just truncated to the sample name

    4. RGID is then extended from index1.index2.lane to index1.index2.lane.run_id.unique_id

    The modified fastq_list_rows object is then returned

    :param event:
    :param context:
    :return: fastq container
    """

    logger.info(f"Start processing fastq list rows event")
    logger.debug(libjson.dumps(event))

    fastq_list_rows: list = event['fastq_list_rows']
    sequencing_run: str = event["sequencing_run"]

    # Easier to work this as a dataframe and iterate through each row
    # Particularly if something comes up where we need to work over a column
    fastq_list_df = pd.DataFrame(fastq_list_rows)

    # Iterate thorugh each row and update fastq_list_row object
    new_rows = []
    for index, row in fastq_list_df.iterrows():
        # Copy out old row
        new_row = row.copy()

        # Check read_1 and read_2 exist on gds and set to location attributes
        check_gds_file(row['read_1']["location"])
        new_row["read_1"] = row["read_1"]["location"]

        # First read_2 exists
        if "read_2" in row.keys() and not row["read_2"] is None and not row["read_2"] == "":
            check_gds_file(row['read_2']["location"])
            new_row["read_2"] = row["read_2"]["location"]
        else:
            # Set to null value
            new_row["read_2"] = None

        # Get values from sample regex
        sample_match = SAMPLE_REGEX_OBJS["unique_id"].match(row["rgsm"])

        # Check sample_match isn't none (otherwise we're in trouble)
        if sample_match is None:
            logger.warning("Could not match sample \"{}\" to split sample and library, skipping sample".format(row["rgsm"]))
            continue

        # Get new rgsm value
        new_row["rgsm"] = sample_match.group(1)

        # Get new rglb value and then split on topup
        rglb = sample_match.group(2)

        # Split out _topup$
        rglb = SAMPLE_REGEX_OBJS["topup"].split(rglb, 1)[0]

        # Split out _rerun$
        rglb = SAMPLE_REGEX_OBJS["rerun"].split(rglb, 1)[0]

        # Assign to new row attr
        new_row["rglb"] = rglb

        # Get new rgid value, which now appends the run id and the unique rgsm value for this sample
        # This becomes 'index1.index2.lane.seq_id.uniq_id'
        new_row["rgid"] = ".".join(map(str, [
            row["rgid"], sequencing_run, row["rgsm"]
        ]))

        new_rows.append(new_row)

    # Convert to df and then to list[dict]
    new_fastq_list_df = pd.concat(new_rows, axis="columns").transpose()

    # Convert back to a list of dicts
    new_fastq_list = new_fastq_list_df.to_dict(orient="records")

    return new_fastq_list
