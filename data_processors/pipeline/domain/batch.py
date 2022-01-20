# -*- coding: utf-8 -*-
"""batch-ing domain module

Domain models related to Workflow Automation.
See domain package __init__.py doc string.
See orchestration package __init__.py doc string.

Impl Note:
Batcher encapsulate Batch creation logics. It is a loose builder (See Creational pattern).
Once instantiated, you get a batcher object that has a Batch and related BatchRun.
You can use it for those jobs (workflows) that wish to run in batch manner and batch notification.

Note that _import_ free here! And also passing X_srv module into the constructor. Power of Python duck typing! And, you
may wish to read a bit about topics in Polymorphism, Dependency Injection (DI) and Inversion of Control (IoC) patterns.
We could import those respective classes and hint the typing to the caller. Otherwise, you know what you are passing
into the constructor by "naming" convention. That's the lore of pure Pythonic-ism "readability".
"""


class Batcher:

    def __init__(self, workflow, run_step: str, batch_srv, fastq_srv, logger=None):
        self.workflow = workflow
        self.run_step = run_step
        self.batch_srv = batch_srv
        self.fastq_srv = fastq_srv
        self.logger = logger

        # things that this Batcher built and hold
        self.batch = None
        self.batch_run = None
        self.sqr = None

        # build it
        self._build()

    # --- internal behaviours

    def _build(self):
        self.sqr = self.workflow.sequence_run

        # create a batch if not exist
        batch_name = self.sqr.name if self.sqr else f"{self.workflow.type_name}__{self.workflow.wfr_id}"
        self.batch = self.batch_srv.get_or_create_batch(name=batch_name, created_by=self.workflow.wfr_id)

        # register a new batch run for this_batch's run step
        self.batch_run = self.batch_srv.skip_or_create_batch_run(
            batch=self.batch,
            run_step=self.run_step,
        )

        if self.batch_run is not None:
            self._prepare_batch_context()

    def _prepare_batch_context(self):
        if self.batch.context_data is None:
            # cache batch context data in db
            fastq_list_rows = self.fastq_srv.get_fastq_list_row_by_sequence_name(self.sqr.name)
            self.batch = self.batch_srv.update_batch(self.batch.id, context_data=fastq_list_rows)

    # --- external behaviours

    def get_skip_message(self):
        # skip the request if there is on going existing batch_run for the same batch run step
        # this is especially to fence off duplicate ICA WES events hitting multiple time to our ICA event lambda
        msg = f"SKIP. THERE IS EXISTING ON GOING RUN FOR BATCH " \
              f"ID: {self.batch.id}, NAME: {self.batch.name}, CREATED_BY: {self.batch.created_by}"
        if self.logger is not None:
            self.logger.warning(msg)
        return {'message': msg}

    def reset_batch_run(self):
        self.batch_run = self.batch_srv.reset_batch_run(self.batch_run.id)

    def get_status(self):
        return {
            'batch_id': self.batch.id,
            'batch_name': self.batch.name,
            'batch_created_by': self.batch.created_by,
            'batch_run_id': self.batch_run.id,
            'batch_run_step': self.batch_run.step,
            'batch_run_status': "RUNNING" if self.batch_run.running else "NOT_RUNNING"
        }


class BatchRuleError(ValueError):
    pass


class BatchRule:
    """
    BatchRule model that check some state must conform in wrapped this_library. Implement your rule that start with
    must_XX expression. Raise BatchRuleError if not conformant. Otherwise, return itself for chain validation.

    NOTE: Here we injected this_library as in its primitive form (i.e. string) and accompanying libraryrun_srv service
    module to reconstruct LibraryRun state from database. We could build this LibraryRun instance outside at the caller.
    But, we wish to encapsulate all business logic together here to avoid repetitive building at the caller side.
    """

    def __init__(self, batcher: Batcher, this_library: str, libraryrun_srv):
        self.batcher = batcher
        self.this_library = this_library
        self.libraryrun_srv = libraryrun_srv

        # derived attributes for convenience
        self.run_id = self.batcher.sqr.run_id if self.batcher.sqr else None
        self.instrument_run_id = self.batcher.sqr.name if self.batcher.sqr else None
        self.run_step = self.batcher.run_step

    def must_not_have_succeeded_runs(self):
        from data_processors.pipeline.domain.workflow import WorkflowStatus
        succeeded_library_runs = self.libraryrun_srv.get_library_runs(
            library_id=self.this_library,
            run_id=self.run_id,
            instrument_run_id=self.instrument_run_id,
            workflows__type_name=self.run_step,
            workflows__end_status=WorkflowStatus.SUCCEEDED.value,
        )

        # if succeeded_library_runs is not empty then there must have some Runs already succeeded for this_library
        if succeeded_library_runs:
            raise BatchRuleError(f"library_id {self.this_library} for {self.instrument_run_id} has "
                                 f"{WorkflowStatus.SUCCEEDED.value} {self.run_step} workflow run")

        return self
