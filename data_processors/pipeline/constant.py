# -*- coding: utf-8 -*-
"""module for pipeline constants

Let's be Pythonic 💪 let's not mutate CAP_VAR elsewhere!
Consider Enum, if there's a need for (name, value) and better protected tuple pair.
Or consider Helper class-ing where composite builder is needed.
"""
from enum import Enum


IAP_BASE_URL = "https://aps2.platform.illumina.com"
IAP_GDS_FASTQ_VOL = "/iap/gds/fastq_vol"
IAP_WORKFLOW_PREFIX = "/iap/workflow"
IAP_JWT_TOKEN = "/iap/jwt-token"

TRACKING_SHEET_ID = "/umccr/google/drive/tracking_sheet_id"
GDRIVE_SERVICE_ACCOUNT = "/umccr/google/drive/lims_service_account_json"

SQS_GERMLINE_QUEUE_ARN = "/data_portal/backend/sqs_germline_queue_arn"
SQS_NOTIFICATION_QUEUE_ARN = "/data_portal/backend/sqs_notification_queue_arn"


class SampleSheetCSV(Enum):
    NAME = "SampleSheet"
    FILENAME = "SampleSheet.csv"


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


class Helper(object):
    pass


class WorkflowHelper(Helper):
    def __init__(self, name):
        self.name = name

    def get_ssm_key_id(self):
        return f"{IAP_WORKFLOW_PREFIX}/{self.name}/id"

    def get_ssm_key_version(self):
        return f"{IAP_WORKFLOW_PREFIX}/{self.name}/version"

    def get_ssm_key_input(self):
        return f"{IAP_WORKFLOW_PREFIX}/{self.name}/input"

    def get_ssm_key_engine_parameters(self):
        return f"{IAP_WORKFLOW_PREFIX}/{self.name}/engine_parameters"
