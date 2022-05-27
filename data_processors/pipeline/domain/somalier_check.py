# -*- coding: utf-8 -*-
"""somalier check domain module

Domain models related to Somalier Check of bam files
This is typically used to test the similarity between two bam files

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from abc import ABC, abstractmethod
from typing import List

from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.lambdas import somalier_check
from data_processors.pipeline.services import workflow_srv, sequencerun_srv, metadata_srv


class SomalierCheck(ABC):
    """Somalier Check based on Abstract Classing"""

    def __init__(self, somalier_check_data=None):
        super().__init__()
        self._data = somalier_check_data
        self._related_bams = []
        self.run_somalier_check_lambda()
        self.get_related_bams()

    def run_somalier_check_lambda(self):
        self._related_bams = somalier_check.handler(self._data, context=None)

    def get_related_bams(self):
        return self._related_bams
