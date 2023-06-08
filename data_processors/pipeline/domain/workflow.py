# -*- coding: utf-8 -*-
"""workflow domain module

Domain models related to Workflow Automation.
See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from uuid import uuid4

from libumccr import libjson
from libumccr.aws import libssm

from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SampleSheetCSV(Enum):
    NAME = "SampleSheet"
    FILENAME = "SampleSheet.csv"


class WorkflowType(Enum):
    BCL_CONVERT = "bcl_convert"
    DRAGEN_WGTS_QC = "wgts_alignment_qc"  # Placeholder workflow type
    DRAGEN_WGS_QC = "wgs_alignment_qc"
    DRAGEN_WTS_QC = "wts_alignment_qc"
    TUMOR_NORMAL = "wgs_tumor_normal"
    DRAGEN_TSO_CTDNA = "tso_ctdna_tumor_only"
    DRAGEN_WTS = "wts_tumor_only"
    UMCCRISE = "umccrise"
    RNASUM = "rnasum"

    @classmethod
    def from_value(cls, value):
        if value == cls.BCL_CONVERT.value:
            return cls.BCL_CONVERT
        elif value == cls.DRAGEN_WGTS_QC.value:
            return cls.DRAGEN_WGTS_QC
        elif value == cls.DRAGEN_WGS_QC.value:
            return cls.DRAGEN_WGS_QC
        elif value == cls.DRAGEN_WTS_QC.value:
            return cls.DRAGEN_WTS_QC
        elif value == cls.TUMOR_NORMAL.value:
            return cls.TUMOR_NORMAL
        elif value == cls.DRAGEN_TSO_CTDNA.value:
            return cls.DRAGEN_TSO_CTDNA
        elif value == cls.DRAGEN_WTS.value:
            return cls.DRAGEN_WTS
        elif value == cls.UMCCRISE.value:
            return cls.UMCCRISE
        elif value == cls.RNASUM.value:
            return cls.RNASUM
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
            "workDirectory": self.construct_workdir(target_id=target_id),
            "outputDirectory": self.construct_outdir(target_id=target_id)
        }

    def construct_workflow_name(self, seq_name: str):
        # pattern: [AUTOMATION_PREFIX]__[WORKFLOW_TYPE]__[WORKFLOW_SPECIFIC_PART]__[PORTAL_RUN_ID]
        return f"{WorkflowHelper.prefix}__{self.type.value}__{seq_name}__{self.portal_run_id}"


class SecondaryAnalysisHelper(WorkflowHelper):

    def __init__(self, type_: WorkflowType):
        allowed_workflow_types = [
            WorkflowType.DRAGEN_WGS_QC,
            WorkflowType.DRAGEN_TSO_CTDNA,
            WorkflowType.DRAGEN_WTS,
            WorkflowType.TUMOR_NORMAL,
            WorkflowType.UMCCRISE,
            WorkflowType.RNASUM
        ]
        if type_ not in allowed_workflow_types:
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


class SequenceRuleError(ValueError):
    pass


class SequenceRule:

    def __init__(self, this_sequence):
        self.this_sequence = this_sequence

    def must_not_emergency_stop(self):
        """
        emergency_stop_list - is simple registry list that
            - is in JSON format
            - store in SSM param store
            - contains Sequence instrument run ID(s) e.g. ["200612_A01052_0017_BH5LYWDSXY"]

        Business rule:
        If this_sequence is found in the emergency stop list then it will stop any further processing.
        Otherwise, emergency stop list should be empty list [].

        Here is an example to set emergency_stop_list for Run 200612_A01052_0017_BH5LYWDSXY.
        To reset, simply payload value to the empty list [].

            aws ssm put-parameter \
              --name "/iap/workflow/emergency_stop_list" \
              --type "String" \
              --value "[\"200612_A01052_0017_BH5LYWDSXY\"]" \
              --overwrite \
              --profile dev
        """
        try:
            emergency_stop_list_json = libssm.get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/emergency_stop_list")
            emergency_stop_list = libjson.loads(emergency_stop_list_json)
        except Exception as e:
            # If any exception found, log warning and proceed
            logger.warning(f"Cannot read emergency_stop_list from SSM param. Exception: {e}")
            emergency_stop_list = []

        if self.this_sequence.instrument_run_id in emergency_stop_list:
            raise SequenceRuleError(f"Sequence {self.this_sequence.instrument_run_id} is marked for emergency stop.")

        return self


class WorkflowRuleError(ValueError):
    pass


class WorkflowRule:
    """
    WorkflowRule model that check some state must conform in wrapped Workflow. Each rule start with must_XX expression.
    If rule is passed, return itself back. So that we can perform chain validation. Otherwise, it raises exception and
    halt the app. It is desirable to halt the execution as continue doing so is harmful to the system.
    """

    def __init__(self, this_workflow):
        self.workflow = this_workflow

    def must_associate_sequence_run(self):
        this_sqr = self.workflow.sequence_run
        if this_sqr is None:
            raise WorkflowRuleError(f"Workflow {self.workflow.type_name} wfr_id: '{self.workflow.wfr_id}' must be "
                                    f"associated with a SequenceRun. SequenceRun is: {this_sqr}")
        return self

    def must_have_output(self):
        # use case e.g. bcl convert workflow run must have output in order to continue next step(s)
        if self.workflow.output is None:
            raise WorkflowRuleError(f"Workflow '{self.workflow.wfr_id}' output is None")
        return self


class LabMetadataRuleError(ValueError):
    pass


class LabMetadataRule:
    """
    LabMetadataRule model that check some state must conform in wrapped LabMetadata. Implement your rule that start with
    must_XX expression. Raise LabMetadataRuleError if not conformant. Otherwise, return itself for chain validation.

    NOTE: The aspect here is just "metadata" itself as-is validation. For LibraryRun aspect, see LibraryRunRule.
    """

    def __init__(self, this_metadata):
        if this_metadata is None:
            raise LabMetadataRuleError(f"No metadata.")
        self.this_metadata = this_metadata

    def must_set_workflow(self):
        """Workflow can not be null or empty"""
        if self.this_metadata.workflow is None or str(self.this_metadata.workflow) == "":
            raise LabMetadataRuleError(f"Workflow is not defined.")
        return self

    def must_not_manual(self):
        from data_portal.models.labmetadata import LabMetadataWorkflow
        if self.this_metadata.workflow.lower() == LabMetadataWorkflow.MANUAL.value.lower():
            raise LabMetadataRuleError(f"Workflow is set to manual.")
        return self

    def must_not_qc(self):
        """Demultiplex and run QC on FASTQ (once on ICA that includes DRAGEN alignment for better stats)"""
        from data_portal.models.labmetadata import LabMetadataWorkflow
        if self.this_metadata.workflow.lower() == LabMetadataWorkflow.QC.value.lower():
            raise LabMetadataRuleError(f"Workflow is set to QC.")
        return self

    def must_not_bcl(self):
        """Do not demultiplex, concept; keep flowcell as-is"""
        from data_portal.models.labmetadata import LabMetadataWorkflow
        if self.this_metadata.workflow.lower() == LabMetadataWorkflow.BCL.value.lower():
            raise LabMetadataRuleError(f"Workflow is set to BCL.")
        return self

    def must_not_ntc(self):
        from data_portal.models.labmetadata import LabMetadataPhenotype
        if self.this_metadata.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower():
            raise LabMetadataRuleError(f"Negative-control sample.")
        return self

    def must_be_tumor(self):
        """Sample Phenotype must be tumor"""
        from data_portal.models.labmetadata import LabMetadataPhenotype
        if self.this_metadata.phenotype.lower() != LabMetadataPhenotype.TUMOR.value.lower():
            raise LabMetadataRuleError(f"Not tumor sample.")
        return self

    def must_be_tumor_or_ntc(self):
        """Sample Phenotype must be tumor"""
        from data_portal.models.labmetadata import LabMetadataPhenotype
        if not (
                self.this_metadata.phenotype.lower() == LabMetadataPhenotype.TUMOR.value.lower() or
                self.this_metadata.phenotype.lower() == LabMetadataPhenotype.N_CONTROL.value.lower()
        ):
            raise LabMetadataRuleError(f"Not tumor sample or NTC sample.")
        return self

    def must_be_wgs(self):
        from data_portal.models.labmetadata import LabMetadataType
        if self.this_metadata.type.lower() != LabMetadataType.WGS.value.lower():
            raise LabMetadataRuleError(f"'WGS' != '{self.this_metadata.type}'.")
        return self

    def must_be_wgts(self):
        from data_portal.models.labmetadata import LabMetadataType
        wgts_values = [ LabMetadataType.WGS.value.lower(), LabMetadataType.WTS.value.lower() ]
        if self.this_metadata.type.lower() not in wgts_values:
            raise LabMetadataRuleError(f"'WGS' or 'WTS' != '{self.this_metadata.type}'.")
        return self

    def must_be_wts(self):
        from data_portal.models.labmetadata import LabMetadataType
        if self.this_metadata.type.lower() != LabMetadataType.WTS.value.lower():
            raise LabMetadataRuleError(f"'WTS' != '{self.this_metadata.type}'.")
        return self

    def must_be_cttso_ctdna(self):
        from data_portal.models.labmetadata import LabMetadataType, LabMetadataAssay
        if not (self.this_metadata.type.lower() == LabMetadataType.CT_DNA.value.lower() and
                self.this_metadata.assay.lower() == LabMetadataAssay.CT_TSO.value.lower()):
            raise LabMetadataRuleError(
                f"Type: 'ctDNA' != '{self.this_metadata.type}' or Assay: 'ctTSO' != '{self.this_metadata.assay}'"
            )
        return self


class LibraryRunRuleError(ValueError):
    pass


class LibraryRunRule:
    """
    LibraryRunRule model that check some state must conform in wrapped LibraryRun. Implement your rule that start with
    must_XX expression. Raise LibraryRunRuleError if not conformant. Otherwise, return itself for chain validation.

    NOTE: this_metadata is the corresponding counterpart of LabMetadata that compliment for validation purpose. You
    may wish to encapsulate services for more control with lookup. In that case, see batch module BatchRule for example
    as injecting service dependencies pattern... Otherwise, reconstruction of wrapped instances are not a concern of
    this LibraryRunRule implementations. No strict practice here. Just use-as-see-fit for the better maintainable code...
    """

    def __init__(self, this_library, this_metadata):
        self.this_library = this_library
        self.this_metadata = this_metadata

    def must_pass_qc(self):
        # TODO
        return self

    def must_have_acceptable_coverage_yield(self):
        # TODO
        return self

    def must_be_valid_for_analysis(self):
        # TODO
        return self


class ICAResourceType(Enum):
    # See https://illumina.gitbook.io/ica-v1/analysis/a-taskexecution#type-and-size for more information
    STANDARD = "standard"
    HI_CPU = "standardHiCpu"
    HI_MEM = "standardHiMem"
    FPGA = "fpga"


class ICAResourceSize(Enum):
    # See https://illumina.gitbook.io/ica-v1/analysis/a-taskexecution#type-and-size for more information
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    XLARGE = "xlarge"
    XXLARGE = "xxlarge"


class ICAResourceOverridesStep:
    def __init__(self, step_id, resource_type: ICAResourceType, resource_size: ICAResourceSize):
        self.step_id = step_id
        self.resource_type = resource_type.value
        self.resource_size = resource_size.value

    def get_resource_requirement_overrides(self):
        return {
            "ResourceRequirement": {
                "https://platform.illumina.com/rdf/ica/resources": {
                    "size": self.resource_size,
                    "type": self.resource_type
                }
            }
        }
