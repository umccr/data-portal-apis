import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from django.core.serializers.json import DjangoJSONEncoder
from libiap.openapi import libwes

from data_portal.models import SequenceRun, Workflow
from data_processors import services
from data_processors.pipeline.dto import WorkflowType, FastQReadType, FastQ
from data_processors.pipeline.eps import WESInterface
from data_processors.pipeline.factory import FastQBuilder
from data_processors.pipeline.input import BCLConvertInput, GermlineInput
from utils import libssm

logger = logging.getLogger(__name__)


class WorkflowSpecification(object):

    def __init__(self):
        self.workflow_type: Optional[WorkflowType] = None
        self.fastq_read_type: Optional[FastQReadType] = None
        self.sequence_run: Optional[SequenceRun] = None
        self.parents: Optional[List[Workflow]] = None


class WorkflowDomainModel(object):

    def __init__(self, spec: WorkflowSpecification):
        assert spec.workflow_type is not None, "Workflow type must be defined in spec"
        self._workflow_type: WorkflowType = spec.workflow_type
        self._sqr: Optional[SequenceRun] = spec.sequence_run
        self._parents: Optional[List[Workflow]] = spec.parents
        self._fastq_read_type: Optional[FastQReadType] = spec.fastq_read_type

        self._iap_workflow_prefix = "/iap/workflow"
        ssm_key_iap_workflow_prefix = os.getenv("SSM_KEY_NAME_IAP_WORKFLOW_PREFIX", None)
        if ssm_key_iap_workflow_prefix:
            self._iap_workflow_prefix = libssm.SSMParamStore(ssm_key_iap_workflow_prefix).value

        iap_wes_workflow_id = os.getenv('IAP_WES_WORKFLOW_ID', None)  # mainly for test cases
        if iap_wes_workflow_id is None:
            iap_wes_workflow_id = self.build_ssm_path("id").value  # typically load it from ssm param store
        self._workflow_id: str = iap_wes_workflow_id

        iap_wes_workflow_version_name = os.getenv("IAP_WES_WORKFLOW_VERSION_NAME")
        if iap_wes_workflow_version_name is None:
            iap_wes_workflow_version_name = self.build_ssm_path("version").value
        self._workflow_version: str = iap_wes_workflow_version_name

        iap_wes_workflow_input_json = self.build_ssm_path("input").value  # must load input from ssm param store
        self._workflow_input: dict = json.loads(iap_wes_workflow_input_json)

        self.wfr_name = None
        self.wfr_id = None
        self.wfv_id = None
        self.start = None
        self.output = None
        self.end = None
        self.end_status = None

    def asdict(self) -> dict:
        """
        Export what can be revealed to external as dict representation of this model.
        Values are at point-in-time calling of this WorkflowDomainModel object state.

        :return: dict
        """
        return {
            'wfr_name': self.wfr_name,
            'wfl_id': self._workflow_id,
            'wfr_id': self.wfr_id,
            'wfv_id': self.wfv_id,
            'type': self._workflow_type,
            'version': self._workflow_version,
            'input': self._workflow_input,
            'start': self.start,
            'output': self.output,
            'end': self.end,
            'end_status': self.end_status,
            'sequence_run': self._sqr,
            'parents': self._parents,
            'fastq_read_type': self._fastq_read_type,
        }

    @property
    def workflow_type(self) -> WorkflowType:
        return self._workflow_type

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    @property
    def workflow_version(self) -> str:
        return self._workflow_version

    @property
    def workflow_input(self) -> dict:
        return self._workflow_input

    @property
    def sqr(self) -> SequenceRun:
        return self._sqr

    sequence_run: sqr

    def build_ssm_path(self, attribute_name) -> libssm.SSMParamStore:
        return libssm.SSMParamStore(f"{self._iap_workflow_prefix}/{self._workflow_type.value}/{attribute_name}")

    def launch(self):
        if self._workflow_type == WorkflowType.BCL_CONVERT:
            assert self._sqr is not None, f"Must provide SequenceRun for {WorkflowType.BCL_CONVERT.value} workflow"
            self._workflow_input = BCLConvertInput(self._workflow_input, self._sqr).get_input()
            WorkflowLaunch(self)

        elif self._workflow_type == WorkflowType.GERMLINE:
            assert len(self._parents) == 1, f"Workflow type {self._workflow_type} only support 1 parent a.t.m"

            parent = self._parents[0]
            assert parent is not None, f"Required completed {WorkflowType.BCL_CONVERT} workflow"
            assert parent.type_name == WorkflowType.BCL_CONVERT.name, f"Parent must be {WorkflowType.BCL_CONVERT}"

            if self._fastq_read_type is None:
                logger.warning(f"FastQReadType is undefined. Assume {FastQReadType.PAIRED_END} "
                               f"for {WorkflowType.GERMLINE}")
                self._fastq_read_type = FastQReadType.PAIRED_END

            fastq: FastQ = FastQBuilder(parent).build()
            for sample_id, bag in fastq.fastq_map.items():
                fastq_list = bag['fastq_list']
                if self._fastq_read_type == FastQReadType.PAIRED_END:
                    if len(fastq_list) > FastQReadType.PAIRED_END.value:
                        # log and skip
                        logger.warning(f"SKIP SAMPLE ID `{sample_id}` {WorkflowType.GERMLINE.name} WORKFLOW LAUNCH. "
                                       f"EXPECTING {self._fastq_read_type.PAIRED_END.value} FASTQ FILES FOR "
                                       f"{self._fastq_read_type}. FOUND: {fastq_list}")
                        continue

                    f1 = f"{fastq.gds_path}/{fastq_list[0]}"
                    f2 = f"{fastq.gds_path}/{fastq_list[1]}"
                    self._workflow_input = GermlineInput(self._workflow_input, f1, f2, sample_id).get_input()

                elif self._fastq_read_type == FastQReadType.PAIRED_END_TWO_LANES_SPLIT:
                    raise NotImplementedError  # TODO

                WorkflowLaunch(self)

    def update(self, workflow: Workflow):
        assert self._workflow_id == workflow.wfl_id, "Workflow ID mis-match"
        assert self._workflow_version == workflow.version, "Workflow Version mis-match"
        assert self._workflow_type.name == workflow.type_name, "Workflow Type mis-match"
        self._workflow_id = workflow.wfl_id
        self.wfr_id = workflow.wfr_id
        self.wfv_id = workflow.wfv_id
        WorkflowUpdate(self)

    def save(self):
        services.create_or_update_workflow(self.asdict())


class WorkflowUpdate(WESInterface):

    def __init__(self, model: WorkflowDomainModel):
        with self.api_client:
            run_api = libwes.WorkflowRunsApi(self.api_client)
            wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id=model.wfr_id)
            model.output = wfl_run.output
            model.end = wfl_run.time_stopped
            model.end_status = wfl_run.status
            model.save()


class WorkflowLaunch(WESInterface):

    def __init__(self, model: WorkflowDomainModel):

        utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        if model.sqr:
            model.wfr_name = f"umccr__{model.workflow_type.value}__{model.sqr.name}__{model.sqr.run_id}__{utc_now_ts}"
        else:
            model.wfr_name = f"umccr__{model.workflow_type.value}__None__None__{utc_now_ts}"  # just in case sqr is None

        with self.api_client:
            version_api = libwes.WorkflowVersionsApi(self.api_client)
            workflow_id = model.workflow_id
            version_name = model.workflow_version
            body = libwes.LaunchWorkflowVersionRequest(
                name=model.wfr_name,
                input=model.workflow_input,
            )

            try:
                logger.info(f"LAUNCHING WORKFLOW_ID: {workflow_id}, VERSION_NAME: {version_name}, "
                            f"INPUT: \n{json.dumps(body.to_dict(), cls=DjangoJSONEncoder)}")
                wfl_run: libwes.WorkflowRun = version_api.launch_workflow_version(workflow_id, version_name, body=body)
                logger.info(f"WORKFLOW LAUNCH SUCCESS: \n{json.dumps(wfl_run.to_dict(), cls=DjangoJSONEncoder)}")

                model.start = wfl_run.time_started if wfl_run.time_started else datetime.utcnow()
                model.wfr_id = wfl_run.id
                wfv: libwes.WorkflowVersionCompact = wfl_run.workflow_version
                model.wfv_id = wfv.id
                model.save()
            except libwes.ApiException as e:
                logger.error(f"Exception when calling launch_workflow_version: \n{e}")
