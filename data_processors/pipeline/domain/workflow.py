# -*- coding: utf-8 -*-
"""workflow domain module

Domain models related to Workflow Automation.
See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from datetime import datetime, timezone
from enum import Enum

from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from utils import libssm, libdt
from pathlib import Path

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
    DRAGEN_WGS_QC = "dragen_wgs_qc"
    TUMOR_NORMAL = "tumor_normal"
    DRAGEN_TSO_CTDNA = "dragen_tso_ctdna"


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


class Helper(object):
    pass


class EngineParametersHelper(Helper):
    """
    Given a workflow type, provide methods for generating the full engine parameter values
    """
    def __init__(self, type_: WorkflowType):
        self.type = type_

    # Get roots from ssm
    @staticmethod
    def get_ssm_key_workdir_root():
        # ssm outputs will be gds://production/temp
        return f"{ICA_WORKFLOW_PREFIX}/workdir_root"

    @staticmethod
    def get_ssm_key_output_root():
        # ssm outputs will be gds://production/
        return f"{ICA_WORKFLOW_PREFIX}/output_root"

    def construct_workdir(self, subject_id, timestamp: datetime):
        """
        Construct a work directory given a subject ID and a timestamp
        :return:
        """
        return self.get_ssm_key_workdir_root() + self.get_mid_path(subject_id, timestamp)

    def construct_outdir(self, subject_id, timestamp: datetime):
        """
        Construct an output directory given a subject ID and a timestamp
        :param subject_id:
        :param timestamp:
        :return:
        """
        return self.get_ssm_key_output_root() + self.get_mid_path(subject_id, timestamp)

    def get_mid_path(self, subject_id: str, timestamp: datetime) -> str:
        """
        Implemented in subclasses
        """
        raise NotImplementedError

    def get_engine_params_dict(self, subject_id: str, timestamp: datetime) -> dict:
        """
        Returns the dictionary of workflow engine parameters
        :return:
        """
        return {
            "workDirectory": self.construct_workdir(subject_id, timestamp),
            "outputDirectory": self.construct_outdir(subject_id, timestamp)
        }


class EngineParametersSecondaryAnalysisHelper(EngineParametersHelper):
    """
    Construct the engine parameters for secondary analysis
    """

    def get_mid_path(self, subject_id: str, timestamp: datetime) -> str:
        return str(Path("analysis_data") / subject_id / str(self.type) / libdt.folder_friendly_timestamp(timestamp))


class WorkflowHelper(Helper):
    prefix = "umccr__automated"

    def __init__(self, type_: WorkflowType):
        self.type = type_

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
        # Primary analysis
        if self.type == WorkflowType.BCL_CONVERT:
            seq_name = kwargs['seq_name']
            seq_run_id = kwargs['seq_run_id']
            return f"{WorkflowHelper.prefix}__{self.type.value}__{seq_name}__{seq_run_id}__{utc_now_ts}"
        # Secondary analysis
        elif self.type in [ WorkflowType.DRAGEN_WGS_QC, WorkflowType.DRAGEN_TSO_CTDNA ]:
            seq_name = kwargs['seq_name']
            seq_run_id = kwargs['seq_run_id']
            sample_name = kwargs['sample_name']
            return f"{WorkflowHelper.prefix}__{self.type.value}__{seq_name}__{seq_run_id}__{sample_name}__{utc_now_ts}"
        elif self.type == WorkflowType.TUMOR_NORMAL:
            subject_id = kwargs['subject_id']
            return f"{WorkflowHelper.prefix}__{self.type.value}__{subject_id}__{utc_now_ts}"
        else:
            raise ValueError(f"Unsupported workflow type: {self.type.name}")


class WorkflowRule:
    """
    WorkflowRule model that check some state must conform in wrapped Workflow. Each rule start with must_XX expression.
    If rule is passed, return itself back. So that we can perform chain validation. Otherwise, it raise exception and
    halt the app. It is desirable to halt the execution as continue doing so is harmful to the system.
    """

    def __init__(self, this_workflow):
        self.workflow = this_workflow

    def must_associate_sequence_run(self):
        this_sqr = self.workflow.sequence_run
        if this_sqr is None:
            raise ValueError(f"Workflow {self.workflow.type_name} wfr_id: '{self.workflow.wfr_id}' must be associated "
                             f"with a SequenceRun. SequenceRun is: {this_sqr}")
        return self

    def must_have_output(self):
        # use case e.g. bcl convert workflow run must have output in order to continue next step(s)
        if self.workflow.output is None:
            raise ValueError(f"Workflow '{self.workflow.wfr_id}' output is None")
        return self
