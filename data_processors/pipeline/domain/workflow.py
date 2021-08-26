# -*- coding: utf-8 -*-
"""workflow domain module

Domain models related to Workflow Automation.
See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from utils import libdt, libssm


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


class EngineParameter(ABC):
    """
    Abstract EngineParameter that provide methods for generating engine parameter values
    """

    @staticmethod
    def _sanitize_gds_path_str(gds_path_str: str):
        if not gds_path_str.startswith("gds://"):
            raise ValueError("Must be gds:// URI scheme string")
        return gds_path_str.rstrip("/")

    @staticmethod
    def _sanitize_target_id(target_id: str):
        if target_id is None:
            raise ValueError("target_id must not be none")
        return target_id

    @staticmethod
    def get_ssm_key_workdir_root():
        return f"{ICA_WORKFLOW_PREFIX}/workdir_root"

    @staticmethod
    def get_ssm_key_output_root():
        return f"{ICA_WORKFLOW_PREFIX}/output_root"

    def get_workdir_root(self):
        return libssm.get_ssm_param(self.get_ssm_key_workdir_root())

    def get_output_root(self):
        return libssm.get_ssm_param(self.get_ssm_key_output_root())

    def construct_workdir(self, target_id, timestamp: datetime):
        """
        Construct a work directory given target ID and a timestamp
        """
        workdir_root = self._sanitize_gds_path_str(self.get_workdir_root())
        return workdir_root + "/" + self.get_mid_path(target_id, timestamp)

    def construct_outdir(self, target_id, timestamp: datetime):
        """
        Construct an output directory given target ID and a timestamp
        """
        output_root = self._sanitize_gds_path_str(self.get_output_root())
        return output_root + "/" + self.get_mid_path(target_id, timestamp)

    @abstractmethod
    def get_mid_path(self, subject_id: str, timestamp: datetime) -> str:
        """
        Subclasses must implement
        """
        raise NotImplementedError

    @abstractmethod
    def get_engine_parameters(self, target_id: str, timestamp=datetime.utcnow()) -> dict:
        """
        Subclasses must implement
        """
        raise NotImplementedError


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


class PrimaryDataHelper(WorkflowHelper, EngineParameter):

    def __init__(self, type_: WorkflowType):
        super().__init__(type_)

    def get_mid_path(self, target_id: str, timestamp: datetime) -> str:
        basename = Path("primary_data")
        target_id = self._sanitize_target_id(target_id)
        return str(basename / target_id)

    def get_engine_parameters(self, target_id: str, timestamp=datetime.utcnow()) -> dict:
        """
        Returns the dictionary of workflow engine parameters
        :return:
        """
        target_id = self._sanitize_target_id(target_id)
        return {
            "outputDirectory": self.construct_outdir(target_id, timestamp)
        }


class SecondaryAnalysisHelper(WorkflowHelper, EngineParameter):

    def __init__(self, type_: WorkflowType):
        super().__init__(type_)

    def get_mid_path(self, target_id: str, timestamp: datetime) -> str:
        basename = Path("analysis_data")
        target_id = self._sanitize_target_id(target_id)
        wfl_type = str(self.type.value)
        ts = libdt.folder_friendly_timestamp(timestamp)
        return str(basename / target_id / wfl_type / ts)

    def get_engine_parameters(self, target_id: str, timestamp=datetime.utcnow()) -> dict:
        """
        Returns the dictionary of workflow engine parameters
        :return:
        """
        target_id = self._sanitize_target_id(target_id)
        engine_params = {
            "workDirectory": self.construct_workdir(target_id, timestamp),
            "outputDirectory": self.construct_outdir(target_id, timestamp)
        }

        if self.type == WorkflowType.DRAGEN_TSO_CTDNA:
            # See https://github.com/umccr-illumina/cwl-iap/issues/200
            engine_params.update(maxScatter=8)

        return engine_params


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
