# -*- coding: utf-8 -*-
"""pairing domain module

Domain models related to Genomics Samples Pairing based on Metadata for Secondary Analysis and/or Tertiary Analysis.
This is typically used in our UMCCR ICA Pipeline for Workflow Orchestration and, expose it in related endpoint.

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
from abc import ABC, abstractmethod
from typing import List

from data_portal.models import Workflow
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.orchestration import tumor_normal_step
from data_processors.pipeline.services import workflow_srv, sequence_run_srv, metadata_srv


class Pairing(ABC):
    """Pairing Interface Contract
    by a subject, a sequence run, all outstanding samples, selected samples or selected libraries
    """

    @abstractmethod
    def by_sequence_runs(self):
        pass

    @abstractmethod
    def by_workflows(self):
        pass

    @abstractmethod
    def by_subjects(self):
        pass

    @abstractmethod
    def by_libraries(self):
        pass

    @abstractmethod
    def by_samples(self):
        pass


class CollectionBasedFluentImpl(object):

    def __init__(self):
        self._sequence_runs = set()
        self._workflows = set()
        self._subjects = set()
        self._libraries = set()
        self._samples = set()
        self._job_list = list()

    def add_sequence_run(self, instrument_run_id: str):
        self._sequence_runs.add(instrument_run_id)
        return self

    def add_workflow(self, wfr_id: str):
        self._workflows.add(wfr_id)
        return self

    def add_subject(self, subject_id: str):
        self._subjects.add(subject_id)
        return self

    def add_library(self, library_id: str):
        self._libraries.add(library_id)
        return self

    def add_sample(self, sample_id: str):
        self._samples.add(sample_id)
        return self

    @property
    def sequence_runs(self) -> List:
        return list(self._sequence_runs).copy()

    @property
    def workflows(self) -> List:
        return list(self._workflows).copy()

    @property
    def subjects(self) -> List:
        return list(self._subjects).copy()

    @property
    def libraries(self) -> List:
        return list(self._libraries).copy()

    @property
    def samples(self) -> List:
        return list(self._samples).copy()

    @property
    def job_list(self) -> List:
        return self._job_list.copy()


class TNPairing(Pairing, CollectionBasedFluentImpl):
    """tumor normal pairing collection-based fluent interface implementation"""

    def __init__(self):
        super().__init__()
        self._qc_workflows = list()
        self._meta_list = list()

    def _build(self):
        job_list, subjects, submitting_subjects = tumor_normal_step.prepare_tumor_normal_jobs(self._meta_list)
        self._job_list = self._job_list + job_list

    def by_sequence_runs(self):
        seq_run_list = sequence_run_srv.get_sequence_run_by_instrument_run_ids(self.sequence_runs)
        for seq_run in seq_run_list:
            succeeded: List[Workflow] = workflow_srv.get_succeeded_by_sequence_run(
                sequence_run=seq_run,
                workflow_type=WorkflowType.DRAGEN_WGS_QC
            )
            self._qc_workflows = self._qc_workflows + succeeded
        meta_list, libraries = metadata_srv.get_tn_metadata_by_qc_runs(self._qc_workflows)
        self._meta_list = meta_list
        self._build()

    def by_workflows(self):
        self._qc_workflows = workflow_srv.get_workflows_by_wfr_ids(self.workflows)
        meta_list, libraries = metadata_srv.get_tn_metadata_by_qc_runs(self._qc_workflows)
        self._meta_list = meta_list
        self._build()

    def by_subjects(self):
        meta_list = metadata_srv.get_metadata_by_keywords_in(subjects=self.subjects)
        self._meta_list = meta_list
        self._build()

    def by_libraries(self):
        meta_list = metadata_srv.get_metadata_by_keywords_in(libraries=self.libraries)
        self._meta_list = meta_list
        self._build()

    def by_samples(self):
        meta_list = metadata_srv.get_metadata_by_keywords_in(samples=self.samples)
        self._meta_list = meta_list
        self._build()
