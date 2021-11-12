# -*- coding: utf-8 -*-
"""workflow domain module

Domain models related to Workflow Automation.
See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from uuid import uuid4
import copy


from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from utils import libssm, libjson


class SampleSheetCSV(Enum):
    NAME = "SampleSheet"
    FILENAME = "SampleSheet.csv"


class WorkflowType(Enum):
    BCL_CONVERT = "bcl_convert"
    DRAGEN_WGS_QC = "wgs_alignment_qc"
    TUMOR_NORMAL = "wgs_tumor_normal"
    DRAGEN_TSO_CTDNA = "tso_ctdna_tumor_only"
    DRAGEN_WTS = "wts_tumor_only"

    @classmethod
    def from_value(cls, value):
        if value == cls.BCL_CONVERT.value:
            return cls.BCL_CONVERT
        elif value == cls.DRAGEN_WGS_QC.value:
            return cls.DRAGEN_WGS_QC
        elif value == cls.TUMOR_NORMAL.value:
            return cls.TUMOR_NORMAL
        elif value == cls.DRAGEN_TSO_CTDNA.value:
            return cls.DRAGEN_TSO_CTDNA
        elif value == cls.DRAGEN_WTS.value:
            return cls.DRAGEN_WTS
        else:
            raise ValueError(f"No matching type found for {value}")


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


class WorkflowHelper(ABC):
    prefix = "umccr__automated"
    workdir_root = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/workdir_root")
    output_root = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/output_root")

    def __init__(self, type_: WorkflowType):
        self.type = type_
        self.date_time = datetime.utcnow()
        self.portal_run_id = f"{self.date_time.strftime('%Y%m%d')}{str(uuid4())[:8]}"
        self.workflow_id = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/id")
        self.workflow_version = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/version")
        input_template = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/{self.type.value}/input")
        self.workflow_input = copy.deepcopy(libjson.loads(input_template))

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
    def get_workdir_root():
        return WorkflowHelper.workdir_root

    @staticmethod
    def get_output_root():
        return WorkflowHelper.output_root

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_workflow_version(self) -> str:
        return self.workflow_version

    def get_workflow_input(self) -> dict:
        return self.workflow_input

    def get_portal_run_id(self) -> str:
        return self.portal_run_id

    def construct_workdir(self, target_id, secondary_target_id: str = None):
        """
        Construct a work directory given target ID and a timestamp
        """
        workdir_root = self._sanitize_gds_path_str(self.get_workdir_root())
        return workdir_root + "/" + self.get_mid_path(target_id=target_id, secondary_target_id=secondary_target_id)

    def construct_outdir(self, target_id, secondary_target_id: str = None):
        """
        Construct an output directory given target ID and a timestamp
        """
        output_root = self._sanitize_gds_path_str(self.get_output_root())
        return output_root + "/" + self.get_mid_path(target_id=target_id, secondary_target_id=secondary_target_id)

    @abstractmethod
    def get_mid_path(self, target_id: str, secondary_target_id: str = None) -> str:
        """
        Subclasses must implement
        """
        raise NotImplementedError

    @abstractmethod
    def get_engine_parameters(self, **kwargs) -> dict:
        """
        Subclasses must implement
        """
        raise NotImplementedError

    @abstractmethod
    def construct_workflow_name(self, **kwargs):
        """
        Subclasses must implement
        """
        raise NotImplementedError


class PrimaryDataHelper(WorkflowHelper):

    def __init__(self, type_: WorkflowType):
        if type_ != WorkflowType.BCL_CONVERT:
            raise ValueError(f"Unsupported WorkflowType for Secondary analysis: {type_}")
        super().__init__(type_)

    def get_mid_path(self, target_id: str, secondary_target_id: str = None) -> str:
        basename = Path("primary_data")
        target_id = self._sanitize_target_id(target_id)
        return str(basename / target_id / self.portal_run_id)

    def get_engine_parameters(self, target_id: str) -> dict:
        """
        Returns the dictionary of workflow engine parameters
        :return:
        """
        target_id = self._sanitize_target_id(target_id)
        return {
            "outputDirectory": self.construct_outdir(target_id=target_id)
        }

    def construct_workflow_name(self, seq_name: str):
        # pattern: [AUTOMATION_PREFIX]__[WORKFLOW_TYPE]__[WORKFLOW_SPECIFIC_PART]__[PORTAL_RUN_ID]
        return f"{WorkflowHelper.prefix}__{self.type.value}__{seq_name}__{self.portal_run_id}"


class SecondaryAnalysisHelper(WorkflowHelper):

    def __init__(self, type_: WorkflowType):
        if type_ not in [WorkflowType.DRAGEN_WGS_QC, WorkflowType.DRAGEN_TSO_CTDNA,
                         WorkflowType.DRAGEN_WTS, WorkflowType.TUMOR_NORMAL]:
            raise ValueError(f"Unsupported WorkflowType for Secondary analysis: {type_}")
        super().__init__(type_)

    def get_mid_path(self, target_id: str, secondary_target_id: str = None) -> str:
        basename = Path("analysis_data")
        target_id = self._sanitize_target_id(target_id)
        wfl_type = str(self.type.value)
        if secondary_target_id:
            return str(basename / target_id / wfl_type / self.portal_run_id / secondary_target_id)
        else:
            return str(basename / target_id / wfl_type / self.portal_run_id)

    def get_engine_parameters(self, target_id: str, secondary_target_id=None) -> dict:
        """
        Returns the dictionary of workflow engine parameters
        :return:
        """
        target_id = self._sanitize_target_id(target_id)
        engine_params = {
            "workDirectory": self.construct_workdir(target_id=target_id, secondary_target_id=secondary_target_id),
            "outputDirectory": self.construct_outdir(target_id, secondary_target_id=secondary_target_id)
        }

        if self.type == WorkflowType.DRAGEN_TSO_CTDNA:
            # See https://github.com/umccr-illumina/cwl-iap/issues/200
            engine_params.update(maxScatter=8)

        return engine_params

    def construct_workflow_name(self, subject_id: str, sample_name: str):
        # pattern: [AUTOMATION_PREFIX]__[WORKFLOW_TYPE]__[WORKFLOW_SPECIFIC_PART]__[PORTAL_RUN_ID]
        return f"{WorkflowHelper.prefix}__{self.type.value}__{subject_id}__{sample_name}__{self.portal_run_id}"


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
