# -*- coding: utf-8 -*-
"""manops domain module

Domain models related to Manual of Operations (MANOPS)

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import json
import os
from typing import List
from abc import ABC, abstractmethod
from enum import Enum
from libumccr import aws
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus
from data_processors.pipeline.services.workflow_srv import get_workflows_by_subject_id_and_workflow_type, \
    get_labmetadata_by_wfr_id
import boto3


############################################################
# Improvement: importing from libumccr when it is available
# Ref: https://github.com/umccr/libumccr/commit/cbf781fbd271e63c17dd45bd73751bf4807c1f6c

class LambdaInvocationType(Enum):
    EVENT = 'Event'
    REQUEST_RESPONSE = 'RequestResponse'
    DRY_RUN = 'DryRun'


############################################################


class Report(Enum):
    RNASUM = "rnasum"

    @classmethod
    def from_value(cls, value):
        if value == cls.RNASUM.value:
            return cls.RNASUM
        else:
            raise ValueError(f"No matching type found for {value}")

    @staticmethod
    def to_list():
        return [e.value for e in Report]


class ReportInterface(ABC):

    @abstractmethod
    def generate(self):
        pass


class RNAsumReport(ReportInterface):
    """Triggering RNAsum report via lambda"""

    def __init__(self):
        super().__init__()
        self.wfr_id = ""
        self.subject_id = ""
        self.dataset = ""

    def add_dataset(self, dataset: str):
        self.dataset = dataset

    def add_workflow(self, wfr_id: str):
        self.wfr_id = wfr_id

        matching_labmetadata = get_labmetadata_by_wfr_id(wfr_id=wfr_id)

        # Get subject_id, it should only contain one labmetadata
        self.subject_id = matching_labmetadata[0].subject_id

    def add_workflow_from_subject(self, subject_id: str):
        self.subject_id = subject_id

        workflow_list: List[Workflow] = get_workflows_by_subject_id_and_workflow_type(subject_id=subject_id,
                                                                                      workflow_type=WorkflowType.UMCCRISE
                                                                                      )
        # Set if value exist
        if len(workflow_list) > 0:
            self.wfr_id = workflow_list[0].wfr_id

    def generate(self):
        fn_name = os.getenv('MANOPS_LAMBDA', 'data-portal-api-dev-manops')

        # Check if existing RNAsum workflows is running
        workflow_list: List[Workflow] = get_workflows_by_subject_id_and_workflow_type(subject_id=self.subject_id,
                                                                                      workflow_type=WorkflowType.RNASUM,
                                                                                      workflow_status=WorkflowStatus.RUNNING
                                                                                      )

        if len(workflow_list) > 0:
            # Current RNAsum workflow has run. Terminating
            raise Exception('Unable to run RNAsum workflow while existing running RNAsum workflow is found!')

        payload_json = json.dumps({
            "event_type": Report.RNASUM.value,
            "wfr_id": self.wfr_id,
            "dataset": self.dataset
        })

        lambda_client = boto3.client('lambda')
        lambda_response = lambda_client.invoke(
            FunctionName=fn_name,
            InvocationType=LambdaInvocationType.EVENT.value,
            Payload=payload_json,
        )

        return lambda_response
