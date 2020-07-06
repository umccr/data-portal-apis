from enum import Enum


class ENSEventType(Enum):
    """
    REF:
    https://iap-docs.readme.io/docs/ens_available-events
    https://github.com/umccr-illumina/stratus/issues/22#issuecomment-628028147
    https://github.com/umccr-illumina/stratus/issues/58
    https://iap-docs.readme.io/docs/upload-instrument-runs#run-upload-event
    """
    GDS_FILES = "gds.files"
    BSSH_RUNS = "bssh.runs"
    WES_RUNS = "wes.runs"


class WorkflowType(Enum):
    BCL_CONVERT = "bcl_convert"
    GERMLINE = "germline"


class WorkflowStatus(Enum):
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    ABORTED = "Aborted"


class WorkflowRunEventType(Enum):
    RUNSTARTED = "RunStarted"
    RUNSUCCEEDED = "RunSucceeded"
    RUNFAILED = "RunFailed"
    RUNABORTED = "RunAborted"


class FastQReadType(Enum):
    """Enum as in form of
    FRIENDLY_NAME_OF_SUPPORTED_FASTQ_READ_TYPE = NUMBER_OF_FASTQ_FILES_PRODUCE

    REF:
    FASTQ files explained
    https://sapac.support.illumina.com/bulletins/2016/04/fastq-files-explained.html?langsel=/au/

    bcl2fastq2 Conversion Software v2.20 User Guide
    https://sapac.support.illumina.com/sequencing/sequencing_software/bcl2fastq-conversion-software.html?langsel=/au/

    BCL Convert v3.4 User Guide
    """
    SINGLE_READ = 1
    PAIRED_END = 2
    PAIRED_END_TWO_LANES_SPLIT = 4  # i.e. bcl-convert (or bcl2fastq) run without --no-lane-splitting
