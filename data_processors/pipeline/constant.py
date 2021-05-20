# -*- coding: utf-8 -*-
"""module for pipeline constants

Let's be Pythonic 💪 let's not mutate CAPITAL_VARIABLE elsewhere!
Consider Enum, if there's a need for (name, value) and better protected tuple pair.
Or consider Helper class-ing where composite builder is needed.
"""
from datetime import datetime, timezone
from enum import Enum


ICA_GDS_FASTQ_VOL = "/iap/gds/fastq_vol"
ICA_WORKFLOW_PREFIX = "/iap/workflow"

SQS_TN_QUEUE_ARN = "/data_portal/backend/sqs_tumor_normal_queue_arn"
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
    TUMOR_NORMAL = "tumor_normal"


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
    prefix = "umccr__automated"

    def __init__(self, type: WorkflowType):
        self.type = type

    def get_ssm_key_id(self):
        return f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/id"

    def get_ssm_key_version(self):
        return f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/version"

    def get_ssm_key_input(self):
        return f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/input"

    def get_ssm_key_engine_parameters(self):
        return f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/engine_parameters"

    def construct_workflow_name(self, **kwargs):
        # pattern: [AUTOMATION_PREFIX]__[WORKFLOW_TYPE]__[WORKFLOW_SPECIFIC_PART]__[UTC_TIMESTAMP]
        utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        if self.type == WorkflowType.GERMLINE:
            seq_name = kwargs['seq_name']
            seq_run_id = kwargs['seq_run_id']
            sample_name = kwargs['sample_name']
            return f"{WorkflowHelper.prefix}__{self.type.value}__{seq_name}__{seq_run_id}__{sample_name}__{utc_now_ts}"
        elif self.type == WorkflowType.TUMOR_NORMAL:
            subject_id = kwargs['subject_id']
            return f"{WorkflowHelper.prefix}__{self.type.value}__{subject_id}__{utc_now_ts}"
        elif self.type == WorkflowType.BCL_CONVERT:
            seq_name = kwargs['seq_name']
            seq_run_id = kwargs['seq_run_id']
            return f"{WorkflowHelper.prefix}__{self.type.value}__{seq_name}__{seq_run_id}__{utc_now_ts}"
        else:
            raise ValueError(f"Unsupported workflow type: {self.type.name}")
