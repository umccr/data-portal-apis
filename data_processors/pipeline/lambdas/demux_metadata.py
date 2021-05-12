try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import logging
from contextlib import closing
from tempfile import NamedTemporaryFile

import pandas as pd
from sample_sheet import SampleSheet

from data_processors import const
from data_processors.pipeline import services

from utils import libssm, libjson, libgdrive, gds
from utils.regex_globals import SAMPLE_REGEX_OBJS

SAMPLE_ID_HEADER = "Sample_ID (SampleSheet)"
OVERRIDECYCLES_HEADER = "OverrideCycles"
TYPE_HEADER = "Type"
ASSAY_HEADER = "Assay"
WORKFLOW_HEADER = "Workflow"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def download_metadata(year: str) -> pd.DataFrame:
    """Download the full original metadata from which to extract the required information

    :param year: the sheet in the metadata spreadsheet to load
    """
    lab_sheet_id = libssm.get_secret(const.TRACKING_SHEET_ID)
    account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

    return libgdrive.download_sheet(account_info, lab_sheet_id, sheet=year)


def extract_requested_rows(df, requested_ids: list):
    """Extract the metadata for the requested IDs

    :param df: the dataframe containing the full metadata set (a sheet of the spreadsheet)
    :param requested_ids: a list of IDs for which tho extract the metadata entries
    """
    # filter rows by requested ids
    subset_rows = df[df[SAMPLE_ID_HEADER].isin(requested_ids)]
    # filter colums by data needed for workflow
    subset_cols = subset_rows[[SAMPLE_ID_HEADER, OVERRIDECYCLES_HEADER, TYPE_HEADER, ASSAY_HEADER, WORKFLOW_HEADER]]
    return subset_cols


def handler(event, context):
    """event payload dict
    {
        'gdsVolume': "bssh.xxxx",
        'gdsBasePath': "/Runs/ccc.aaa"
        'gdsSamplesheet': "SampleSheet.csv"
    }

    This lambda would retrieve metadata required for the demultiplexing workflow.
    That is currently metadata from the lab's tracking sheet defining how to group
    samples for demultiplexing. This information will be used to split the initial
    SampleSheet into several, each one with it's own demux options.

    :param event:
    :param context:
    :return: list of dicts containing metadata
    """
    logger.info(f"Start processing demux_metadata event")
    logger.info(libjson.dumps(event))

    # extract list of sample IDs for which to include metadata
    gds_volume = event['gdsVolume']
    gds_base_path = event['gdsBasePath']
    samplesheet_path = event['gdsSamplesheet']

    gds_samplesheet_path = os.path.join(gds_base_path, samplesheet_path)
    logger.info(f"GDS sample sheet path: {gds_samplesheet_path}")

    ntf: NamedTemporaryFile = gds.download_gds_file(gds_volume, gds_samplesheet_path)
    if ntf is None:
        reason = f"Abort extracting metadata process. " \
                f"Can not download sample sheet from GDS: gds://{gds_volume}{gds_samplesheet_path}"
        abort_message = {'message': reason}
        logger.warning(libjson.dumps(abort_message))
        services.notify_outlier(topic="Sample sheet download issue", reason=reason, status="Aborted", event=event)
        return abort_message

    logger.info(f"Local sample sheet path: {ntf.name}")
    sample_ids = set()
    with closing(ntf) as f:
        samplesheet = SampleSheet(f.name)
        for sample in samplesheet:
            sample_ids.add(sample.Sample_ID)

    logger.info(f"Sample IDs: {sample_ids}")

    # Get years
    years = []
    for sample_id in sample_ids:
        # Get sample id and library id
        sample_regex_obj = SAMPLE_REGEX_OBJS["unique_id"].match(sample_id)
        # Get library id
        library_id = sample_regex_obj.group(2)
        # Get year from library id
        year_regex_obj = SAMPLE_REGEX_OBJS["year"].match(library_id)
        # Append year to years
        # Hope this is fixed before 2099
        years.append("20{}".format(year_regex_obj.group(1)))

    years = list(set(years))

    # Download lab metdata sheet for each year
    metadata_dfs = []

    for year in years:
        metadata_dfs.append(download_metadata(year))

    # download the lab metadata sheets
    df_all = pd.concat(metadata_dfs, axis="columns")

    # extract required records from metadata
    requested_metadata_df = extract_requested_rows(df=df_all, requested_ids=list(sample_ids))
    if requested_metadata_df.empty:
        logger.warning(f"Can not extract any associated metadata tracking for sample sheet: "
                       f"gds://{gds_volume}{gds_samplesheet_path}")
    else:
        logger.debug(f"REQUESTED_METADATA_DF: \n{requested_metadata_df}")

    # turn metadata_df into format compatible with workflow input
    # Select, rename, split metadata df
    requested_metadata_df = requested_metadata_df[[SAMPLE_ID_HEADER, OVERRIDECYCLES_HEADER, TYPE_HEADER, ASSAY_HEADER, WORKFLOW_HEADER]]
    # Rename
    requested_metadata_df.rename(columns={
        SAMPLE_ID_HEADER: "sample",
        OVERRIDECYCLES_HEADER: "override_cycles",
        TYPE_HEADER: "type",
        ASSAY_HEADER: "assay",
        WORKFLOW_HEADER: "workflow"
    }, inplace=True)

    # Split by records
    results = requested_metadata_df.to_dict(orient="records")

    logger.info(libjson.dumps(results))

    return results
