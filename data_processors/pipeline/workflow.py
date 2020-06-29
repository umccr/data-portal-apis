import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from libiap.openapi import libwes

from data_portal.models import SequenceRun, Workflow
from data_processors import services
from data_processors.pipeline.dto import WorkflowType, FastQReadType, FastQ, WorkflowStatus
from data_processors.pipeline.eps import WESInterface
from data_processors.pipeline.factory import FastQBuilder
from data_processors.pipeline.input import BCLConvertInput, GermlineInput
from utils import libssm, libslack, libjson

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

        iap_wes_workflow_version_name = os.getenv("IAP_WES_WORKFLOW_VERSION_NAME", None)
        if iap_wes_workflow_version_name is None:
            iap_wes_workflow_version_name = self.build_ssm_path("version").value
        self._workflow_version: str = iap_wes_workflow_version_name

        iap_wes_workflow_input_json = self.build_ssm_path("input").value  # must load input from ssm param store
        self._workflow_input: dict = json.loads(iap_wes_workflow_input_json)

        # construct and format workflow run name convention
        # [RUN_NAME_PREFIX]__[WORKFLOW_TYPE]__[SEQUENCE_RUN_NAME]__[SEQUENCE_RUN_ID]__[UTC_TIMESTAMP]
        _run_name_prefix = "umccr__automated"  # important for wes.runs event filtering startsWith expression if use
        ssm_key_iap_workflow_run_name_prefix = os.getenv("SSM_KEY_NAME_IAP_WORKFLOW_RUN_NAME_PREFIX", None)
        if ssm_key_iap_workflow_run_name_prefix:
            _run_name_prefix = libssm.SSMParamStore(ssm_key_iap_workflow_run_name_prefix).value
        sqr_name = self._sqr.name if self._sqr else "None"
        sqr_run_id = self._sqr.run_id if self._sqr else "None"
        _utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        self.wfr_name = f"{_run_name_prefix}__{self._workflow_type.value}__{sqr_name}__{sqr_run_id}__{_utc_now_ts}"

        self.wfr_id = None
        self.wfv_id = None
        self.start = None
        self.output = None
        self.end = None
        self.end_status = None
        self.sample_name = None
        self.notified = None

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
            'sample_name': self.sample_name,
            'notified': self.notified,
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
            for sample_name, bag in fastq.fastq_map.items():
                fastq_list = bag['fastq_list']
                if self._fastq_read_type == FastQReadType.PAIRED_END:
                    if len(fastq_list) > FastQReadType.PAIRED_END.value:
                        # log and skip
                        logger.warning(f"SKIP SAMPLE '{sample_name}' {WorkflowType.GERMLINE.name} WORKFLOW LAUNCH. "
                                       f"EXPECTING {self._fastq_read_type.PAIRED_END.value} FASTQ FILES FOR "
                                       f"{self._fastq_read_type}. FOUND: {fastq_list}")
                        continue

                    f1 = f"{fastq.gds_path}/{fastq_list[0]}"
                    f2 = f"{fastq.gds_path}/{fastq_list[1]}"
                    self._workflow_input = GermlineInput(self._workflow_input, f1, f2, sample_name).get_input()
                    self.sample_name = sample_name

                elif self._fastq_read_type == FastQReadType.PAIRED_END_TWO_LANES_SPLIT:
                    raise NotImplementedError  # TODO

                WorkflowLaunch(self)

    def synth(self, workflow: Workflow):
        """Synthesize this WorkflowDomainModel instance to prep for updating its state into database"""
        assert self._workflow_id == workflow.wfl_id, "Workflow ID mis-match"
        assert self._workflow_version == workflow.version, "Workflow Version mis-match"
        assert self._workflow_type.name == workflow.type_name, "Workflow Type mis-match"
        self.wfr_id = workflow.wfr_id
        self.wfv_id = workflow.wfv_id
        self.notified = workflow.notified

        # no effect for db update, but for logging purpose
        self.start = workflow.start
        self.sample_name = workflow.sample_name

    def update(self):
        """Update Workflow Run status from WES endpoint"""
        WorkflowUpdate(self)

    def save(self):
        services.create_or_update_workflow(self.asdict())

    def notify(self):
        if not self.end_status:
            logger.info(f"{self.workflow_type.name} '{self.wfr_id}' workflow end status is '{self.end_status}'. "
                        f"Not reporting to Slack!")
            return

        if self.notified:
            logger.info(f"{self.workflow_type.name} '{self.wfr_id}' workflow status '{self.end_status}' is "
                        f"already notified once. Not reporting to Slack!")
            return

        _status: str = self.end_status.lower()

        if _status == WorkflowStatus.RUNNING.value.lower():
            slack_color = libslack.SlackColor.BLUE.value
        elif _status == WorkflowStatus.SUCCEEDED.value.lower():
            slack_color = libslack.SlackColor.GREEN.value
        elif _status == WorkflowStatus.FAILED.value.lower():
            slack_color = libslack.SlackColor.RED.value
        elif _status == WorkflowStatus.ABORTED.value.lower():
            slack_color = libslack.SlackColor.GRAY.value
        else:
            logger.info(f"{self.workflow_type.name} '{self.wfr_id}' workflow unsupported status '{self.end_status}'. "
                        f"Not reporting to Slack!")
            return

        sender = "Portal Workflow Automation"
        topic = f"Run Name: {self.wfr_name}"
        attachments = [
            {
                "fallback": f"RunID: {self.wfr_id}, Status: {_status.upper()}",
                "color": slack_color,
                "pretext": f"Status: {_status.upper()}",
                "title": f"RunID: {self.wfr_id}",
                "text": "Workflow Attributes:",
                "fields": [
                    {
                        "title": "Workflow Type",
                        "value": self.workflow_type.name,
                        "short": True
                    },
                    {
                        "title": "Workflow ID",
                        "value": self.workflow_id,
                        "short": True
                    },
                    {
                        "title": "Workflow Version",
                        "value": self.workflow_version,
                        "short": True
                    },
                    {
                        "title": "Workflow Version ID",
                        "value": self.wfv_id,
                        "short": True
                    },
                    {
                        "title": "Start Time",
                        "value": self.start,
                        "short": True
                    },
                    {
                        "title": "End Time",
                        "value": self.end if self.end else "Not Applicable",
                        "short": True
                    },
                    {
                        "title": "Sequence Run",
                        "value": self.sqr.name if self.sqr else "Not Applicable",
                        "short": True
                    },
                    {
                        "title": "Sample Name",
                        "value": self.sample_name if self.sample_name else "Not Applicable",
                        "short": True
                    },
                ],
                "footer": "Automated Workflow Event",
                "ts": int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
            }
        ]

        resp = libslack.call_slack_webhook(sender, topic, attachments)

        if resp:
            self.notified = True
            self.save()

        return resp


class WorkflowUpdate(WESInterface):

    def __init__(self, model: WorkflowDomainModel):
        super().__init__()
        with self.api_client:
            run_api = libwes.WorkflowRunsApi(self.api_client)
            wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id=model.wfr_id)
            model.output = wfl_run.output
            model.end = wfl_run.time_stopped
            model.end_status = wfl_run.status
            model.save()


class WorkflowLaunch(WESInterface):

    def __init__(self, model: WorkflowDomainModel):
        super().__init__()
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
                            f"INPUT: \n{libjson.dumps(body.to_dict())}")
                wfl_run: libwes.WorkflowRun = version_api.launch_workflow_version(workflow_id, version_name, body=body)
                logger.info(f"WORKFLOW LAUNCH SUCCESS: \n{libjson.dumps(wfl_run.to_dict())}")

                model.start = wfl_run.time_started if wfl_run.time_started else datetime.utcnow()
                model.wfr_id = wfl_run.id
                wfv: libwes.WorkflowVersionCompact = wfl_run.workflow_version
                model.wfv_id = wfv.id
                model.save()
            except libwes.ApiException as e:
                logger.error(f"Exception when calling launch_workflow_version: \n{e}")
