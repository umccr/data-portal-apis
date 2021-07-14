try:
    import unzip_requirements
except ImportError:
    pass

import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')
django.setup()

# ---

import copy
import logging
from tempfile import NamedTemporaryFile
from typing import List

import pandas as pd
from contextlib import closing
from data_portal.models import Workflow, LabMetadata
from data_processors.pipeline.services import notification_srv, sequence_srv, workflow_srv
from data_processors.pipeline.domain.config import ICA_GDS_FASTQ_VOL
from data_processors.pipeline.domain.workflow import WorkflowType, SampleSheetCSV, WorkflowHelper
from data_processors.pipeline.lambdas import wes_handler
from utils import libjson, libssm, libdt, gds
from sample_sheet import SampleSheet


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ADAPTERS_BY_KIT = {
    "truseq": {
        "adapter_read_1": "AGATCGGAAGAGCACACGTCTGAACTCCAGTCA",
        "adapter_read_2": "AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT"
    },
    "nextera": {
        "adapter_read_1": "CTGTCTCTTATACACATCT",
        "adapter_read_2": ""  # null is default and keeps current samplesheet, "" removes value
    },
    "pcr_free_tagmentation": {
        "adapter_read_1": "CTGTCTCTTATACACATCTCCGAGCCCACGAGAC+ATGTGTATAAGAGACA",
        "adapter_read_2": "CTGTCTCTTATACACATCTCGCAGGGGATAGTCAGATGACGCTGCCGACGA+ATGTGTATAAGAGACA"
    },
    "agilent_sureselect_qxt": {
        "adapter_read_1": "CTGTCTCTTGATCACA",
        "adapter_read_2": ""  # null is default and keeps current samplesheet, "" removes value
    },
}


def _build_error_message(reason) -> dict:
    error_message = {'message': reason}
    logger.error(libjson.dumps(error_message))
    return error_message


def validate_metadata(event, settings_by_samples):
    prefix = f"Abort launching BCL Convert workflow."
    suffix = f"after lab metadata tracking sheet and sample sheet filtering step."

    # Check at least one batch is returned
    if settings_by_samples is None or len(settings_by_samples) == 0:
        reason = f"{prefix} settings_by_samples attribute was blank {suffix}"
        notification_srv.notify_outlier(
            topic="No settings by samples found",
            reason=reason,
            status="Aborted",
            event=event
        )
        return reason

    # Check through each settings by samples
    # Make sure each has a batch_name, a non-zero length of samples, and override_cycles in the settings section

    # Check batch name
    for settings_by_samples_batch in settings_by_samples:
        # Check batch_name attribute
        if settings_by_samples_batch.get("batch_name", None) is None or \
                settings_by_samples_batch.get("batch_name") == "":
            reason = f"{prefix} batch {settings_by_samples_batch} did not have \"batch_name\" attribute"
            notification_srv.notify_outlier(topic="Batch Name not found", reason=reason, status="Aborted", event=event)
            return reason

    # Check non-zero length samples
    for settings_by_samples_batch in settings_by_samples:
        batch_name = settings_by_samples_batch.get("batch_name")
        # Check samples length
        if settings_by_samples_batch.get("samples", None) is None or \
                len(settings_by_samples_batch.get("samples")) == 0:
            reason = f"{prefix} no samples found for batch {batch_name}"
            notification_srv.notify_outlier(topic="Samples not found", reason=reason, status="Aborted", event=event)
            return reason
        # Check each samples value is not null
        for sample in settings_by_samples_batch.get("samples"):
            if sample is None or sample == "":
                reason = f"{prefix} found blank sample in {batch_name}"
                notification_srv.notify_outlier(topic="Sample was blank", reason=reason, status="Aborted", event=event)
                return reason

    # Check settings section
    for settings_by_samples_batch in settings_by_samples:
        batch_name = settings_by_samples_batch.get("batch_name")
        # Check settings are present
        if settings_by_samples_batch.get("settings", None) is None or \
                len(settings_by_samples_batch.get("settings")) == 0:
            reason = f"{prefix} no settings found for batch {batch_name}"
            notification_srv.notify_outlier(topic="Settings not found", reason=reason, status="Aborted", event=event)
            return reason
        # Check override cycles section
        if settings_by_samples_batch.get("settings").get("override_cycles", None) is None:
            reason = f"{prefix} no override cycles found for batch {batch_name}"
            notification_srv.notify_outlier(
                topic="Override cycles not found",
                reason=reason,
                status="Aborted",
                event=event
            )
            return reason

    pass


def get_instrument_by_seq_name(seq_name):
    """
    Use a quick regex to split by run id
    """

    # Get flowcell barcode component of run id
    flowcell_barcode = "".join(seq_name.rsplit("_", 1))[-1][1:]

    # Check if there's a hyphen in the flowcell barcode, then it's a MiSeq (for our purposes)
    if "-" in flowcell_barcode:
        return "MiSeq"

    # Check if NovaSeq by last four characters
    if flowcell_barcode[-4:] in ["DMXX", "DMXY", "DRXX", "DRXY", "DSXX", "DSXY"]:
        return "NovaSeq"

    return None


def get_settings_by_instrument_type_assay(instrument, sample_type, assay):
    """
    Here's the potential list of assays to be input
    TsqNano: TruSeq DNA Nano -> Set TruSeq Adapters
    TsqSTR: TruSeq Stranded mRNA -> Set TruSeq Adapters
    NebDNA: NEBNext Multiple Oligos for Illumina -> Set TruSeq Adapters
    NebRNA: NEBNext Multiple Oligos for Illumina -> Set TruSeq Adapters
    AgSsCRE: Agilent SureSelect Clinical Research Exome -> Set Adapters when running on MiSeq
    TSORNA: TSO500 (solid) RNA -> Set adapters, min read length and enable trimming
    TSODNA: TSO500 (solid) DNA -> Set adapters, min read length and enable trimming
    10X-5prime-expression: 10X -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-3prime-expression: 10X -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-VDJ-TCR -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-CITE-feature -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-CITE-hashing -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-ATAC -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-VDJ -> Set create_index_reads, update trimmed_read_length and mask short reads
    10X-CNV -> Set create_index_reads, update trimmed_read_length and mask short reads
    ctTSO: TSO500 (liquid) -> Set adapters, min read length and enable trimming
    PCR-Free-Tagmentation -> Set PCR-Free-Tagmentation Adapters
    """

    # First start with if type == 10X -> return nextera adapters
    if sample_type == "10X":
        # Return nextera adapter
        # settings = ADAPTERS_BY_KIT["nextera"].copy()  # FIXME - 10X adapter settings based on kit used
        # See: https://kb.10xgenomics.com/hc/en-us/articles/360061619811-Why-are-different-index-plates-required-for-different-library-types-
        # We also wish to keep the indexes as reads
        settings = {
          "create_fastq_for_index_reads": True,
          "minimum_trimmed_read_length": 8,
          "mask_short_reads": 8
        }
        return settings

    # Then if assay is TSO -> return TSO parameters
    if assay in ["ctTSO", "TSODNA", "TSORNA"]:
        # Return tso500 samplesheet settings
        settings = ADAPTERS_BY_KIT["truseq"].copy()

        # Add in TSO500 settings
        settings.update(
            {
                "adapter_behavior": "trim",
                "minimum_trimmed_read_length": 35,
                "mask_short_reads": 35
             }
        )
        return settings

    # Then check if starts with Neb, Tsq
    if assay.startswith("Tsq") or assay.startswith("Neb"):
        # Return TruSeq adapters
        settings = ADAPTERS_BY_KIT["truseq"].copy()
        return settings

    # That leaves just agilent or pcr free tagmentation

    # Check if assay if PCR-Free-Tagmentation
    if assay == "PCR-Free-Tagmentation":
        settings = ADAPTERS_BY_KIT["pcr_free_tagmentation"]
        return settings

    # Check if instrument is MiSeq and assay is agilent
    if assay == "AgSsCRE" and instrument == "MiSeq":
        settings = ADAPTERS_BY_KIT["agilent_sureselect_qxt"].copy()
        return settings

    # Otherwise return blank settings set
    return {}


# TODO: extract into utils?
def get_library_id_from_sample_name(sample_name: str):
    # format: samplename_libraryid_extension
    # we are only interested in the library ID
    fragments = sample_name.split("_")
    # if there is an extension, the library ID is the second to last fragment
    if "_topup" in sample_name or "_rerun" in sample_name:
        return fragments[-2]
    # if not, then the library ID is the last fragment
    return fragments[-1]


# TODO: extract SampleSheet handling/utils into separate module?
def get_sample_names_from_samplesheet(gds_volume: str, samplesheet_path: str) -> List[str]:
    if not samplesheet_path.startswith(os.path.sep):
        samplesheet_path = os.path.sep + samplesheet_path
    logger.info(f"Extracting sample names from gds://{gds_volume}{samplesheet_path}")

    ntf: NamedTemporaryFile = gds.download_gds_file(gds_volume, samplesheet_path)
    if ntf is None:
        reason = f"Abort extracting metadata process. " \
                 f"Can not download sample sheet from GDS: gds://{gds_volume}{samplesheet_path}"
        logger.error(reason)
        raise ValueError(reason)

    logger.info(f"Local sample sheet path: {ntf.name}")
    sample_names = set()
    with closing(ntf) as f:
        samplesheet = SampleSheet(f.name)
        for sample in samplesheet:
            sample_names.add(sample.Sample_ID)

    logger.info(f"Extracted sample names: {sample_names}")

    return list(sample_names)


def get_metadata_df(gds_volume: str, samplesheet_path: str) -> pd.DataFrame:
    """Get libraries and metadata associated with this run/SampleSheet"""

    sample_names: List[str] = get_sample_names_from_samplesheet(
        gds_volume=gds_volume,
        samplesheet_path=samplesheet_path
    )

    metadata_df: pd.DataFrame = pd.DataFrame()
    for sample_name in sample_names:
        lib_id = get_library_id_from_sample_name(sample_name)
        try:
            meta: LabMetadata = LabMetadata.objects.get(library_id__iexact=lib_id)
        except LabMetadata.DoesNotExist as err:
            logger.error(f"LabMetadata query for library_id {lib_id} did not find any data! {err}")
            return metadata_df
        except LabMetadata.MultipleObjectsReturned as err:
            logger.error(f"LabMetadata query for library_id {lib_id} found multiple entries! {err}")
            return metadata_df

        new_row = {'sample': sample_name, 'type': meta.type, 'assay': meta.assay,
                   'override_cycles': meta.override_cycles}
        metadata_df = metadata_df.append(new_row, ignore_index=True)
    return metadata_df


def handler(event, context) -> dict:
    """event payload dict
    {
        'gds_volume_name': "bssh.xxxx",
        'gds_folder_path': "/Runs/cccc.gggg",
        'seq_run_id': "yyy",
        'seq_name': "zzz",
    }

    :param event:
    :param context:
    :return: workflow db record id and wfr_id in JSON string
    """

    logger.info(f"Start processing {WorkflowType.BCL_CONVERT.name} event")
    logger.info(libjson.dumps(event))

    gds_volume_name = event['gds_volume_name']
    gds_folder_path = event['gds_folder_path']
    seq_name = event['seq_name']

    run_folder = f"gds://{gds_volume_name}{gds_folder_path}"
    seq_run_id = event.get('seq_run_id', None)

    wfl_helper = WorkflowHelper(WorkflowType.BCL_CONVERT)

    # read input template from parameter store
    input_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_input())
    sample_sheet_gds_folder_path = f"{gds_folder_path}/{SampleSheetCSV.FILENAME.value}"
    sample_sheet_gds_full_path = f"{run_folder}/{SampleSheetCSV.FILENAME.value}"

    metadata_df = get_metadata_df(gds_volume_name, sample_sheet_gds_folder_path)

    # Get instrument type from run id
    instrument = get_instrument_by_seq_name(seq_name)

    workflow_input: dict = copy.deepcopy(libjson.loads(input_template))
    workflow_input['samplesheet']['location'] = sample_sheet_gds_full_path
    workflow_input['bcl_input_directory']['location'] = run_folder
    workflow_input['runfolder_name'] = seq_name

    settings_by_samples = []
    if not metadata_df.empty:
        for (sample_type, assay), sample_group_df in metadata_df.groupby(["type", "assay"]):
            # Get the values in the override cycles column
            override_cycles_list = sample_group_df["override_cycles"].unique().tolist()

            # Get the samplesheet midfix and also the output directory for each batch
            if len(override_cycles_list) == 1:
                batch_name = "{}_{}".format(sample_type, assay)

            # Get the settings for the assay
            assay_settings = get_settings_by_instrument_type_assay(instrument, sample_type, assay)

            for override_cycles in override_cycles_list:

                # Make a copy of the settings
                settings = assay_settings.copy()

                # batch_name is previously defined ONLY if there's one override cycles setting per sample_type and assay
                if not len(override_cycles_list) == 1:
                    batch_name = "{}_{}_{}".format(sample_type, assay, override_cycles.replace(";", "_"))

                # Shrink the samples list to those only with matching override cycles
                samples_list = sample_group_df.query("override_cycles==\"{}\"".format(override_cycles))["sample"].tolist()

                # Overwrite any override cycles settings
                settings["override_cycles"] = override_cycles

                # Append settings
                settings_by_samples.append({
                    "batch_name": batch_name,
                    "samples": samples_list,
                    "settings": settings
                })

    # Validate settings before placing as input
    # Check settings before appending
    failure_reason = validate_metadata(event=event, settings_by_samples=settings_by_samples)
    if failure_reason is not None:
        return _build_error_message(failure_reason)

    # All good, add as input
    workflow_input['settings_by_samples'] = settings_by_samples

    # prepare engine_parameters
    gds_fastq_vol = libssm.get_ssm_param(ICA_GDS_FASTQ_VOL)
    engine_params_template = libssm.get_ssm_param(wfl_helper.get_ssm_key_engine_parameters())
    workflow_engine_params: dict = copy.deepcopy(libjson.loads(engine_params_template))
    workflow_engine_params['outputDirectory'] = f"gds://{gds_fastq_vol}/{seq_name}"

    # read workflow id and version from parameter store
    workflow_id = libssm.get_ssm_param(wfl_helper.get_ssm_key_id())
    workflow_version = libssm.get_ssm_param(wfl_helper.get_ssm_key_version())

    sqr = sequence_srv.get_sequence_run_by_run_id(seq_run_id) if seq_run_id else None

    # construct and format workflow run name convention
    workflow_run_name = wfl_helper.construct_workflow_name(seq_name=seq_name, seq_run_id=seq_run_id)

    wfl_run: dict = wes_handler.launch({
        'workflow_id': workflow_id,
        'workflow_version': workflow_version,
        'workflow_run_name': workflow_run_name,
        'workflow_input': workflow_input,
        'workflow_engine_parameters': workflow_engine_params
    }, context)

    workflow: Workflow = workflow_srv.create_or_update_workflow(
        {
            'wfr_name': workflow_run_name,
            'wfl_id': workflow_id,
            'wfr_id': wfl_run['id'],
            'wfv_id': wfl_run['workflow_version']['id'],
            'type': WorkflowType.BCL_CONVERT,
            'version': workflow_version,
            'input': workflow_input,
            'start': wfl_run.get('time_started'),
            'end_status': wfl_run.get('status'),
            'sequence_run': sqr,
        }
    )

    # notification shall trigger upon wes.run event created action in workflow_update lambda

    result = {
        'id': workflow.id,
        'wfr_id': workflow.wfr_id,
        'wfr_name': workflow.wfr_name,
        'status': workflow.end_status,
        'start': libdt.serializable_datetime(workflow.start),
    }

    logger.info(libjson.dumps(result))

    return result
