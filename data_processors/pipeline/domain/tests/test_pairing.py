from mockito import when

from data_portal.models.workflow import Workflow
from data_portal.tests.factories import DragenWgtsQcWorkflowFactory, LabMetadataFactory
from data_processors.pipeline.domain.pairing import Pairing, CollectionBasedFluentImpl, TNPairing
from data_processors.pipeline.services import sequencerun_srv, workflow_srv, metadata_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger


class PairingUnitTests(PipelineUnitTestCase):

    def test_abc_pairing(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_pairing.PairingUnitTests.test_abc_pairing
        """
        try:
            _ = Pairing()
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(TypeError)

    def test_collection_based_fluent_impl(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_pairing.PairingUnitTests.test_collection_based_fluent_impl
        """
        cbf = CollectionBasedFluentImpl()
        cbf \
            .add_sequence_run("350101_A00130_1999_BHGKLNDSX2") \
            .add_subject("SBJ00001") \
            .add_library("L0000001") \
            .add_sample("MDX000001") \
            .add_workflow("wfr.123")

        logger.info(f"{cbf.sequence_runs}, {cbf.libraries}")
        self.assertEqual("SBJ00001", cbf.subjects[0])

        seq_runs_copy = cbf.sequence_runs
        self.assertTrue(isinstance(seq_runs_copy, list))
        seq_runs_copy.append("ANOTHER")
        self.assertEqual(1, len(cbf.sequence_runs))
        self.assertEqual(2, len(seq_runs_copy))

    def test_by_sequence_runs(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_pairing.PairingUnitTests.test_by_sequence_runs
        """
        mock_workflow: Workflow = DragenWgtsQcWorkflowFactory()
        when(workflow_srv).get_succeeded_by_sequence_run(...).thenReturn([mock_workflow])

        mock_seq_run = mock_workflow.sequence_run
        when(sequencerun_srv).get_sequence_run_by_instrument_run_ids(...).thenReturn([mock_seq_run])

        mock_meta = LabMetadataFactory()
        when(metadata_srv).get_tn_metadata_by_qc_runs(...).thenReturn(([mock_meta], []))

        tn_pairing = TNPairing()
        tn_pairing.add_sequence_run(mock_seq_run.instrument_run_id)
        tn_pairing.by_sequence_runs()
        job_list = tn_pairing.job_list
        logger.info(f"tn_pairing.job_list: {job_list}")
        self.assertEqual(0, len(job_list))

    def test_by_workflows(self):
        pass

    def test_by_subjects(self):
        pass

    def test_by_libraries(self):
        pass

    def test_by_samples(self):
        pass


class PairingIntegrationTests(PipelineIntegrationTestCase):
    pass
