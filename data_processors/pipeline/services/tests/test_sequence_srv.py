from data_portal.models.sequence import Sequence, SequenceStatus
from data_portal.tests.factories import SequenceFactory
from data_processors.pipeline.services import sequence_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class SequenceSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(SequenceSrvUnitTests, self).setUp()

    def test_create_or_update_sequence_from_bssh_event(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_sequence_srv.SequenceSrvUnitTests.test_create_or_update_sequence_from_bssh_event
        """
        mock_run_id = "r.ACGxTAC8mGCtAcgTmITyDA"
        mock_instrument_run_id = "200508_A01052_0001_AC5GT7ACGT"
        mock_date_modified = "2020-05-09T22:17:03.1015272Z"
        mock_status = "New"
        mock_payload = {
            "gdsFolderPath": f"/Runs/{mock_instrument_run_id}_{mock_run_id}",
            "gdsVolumeName": "bssh.acgtacgt498038ed99fa94fe79523959",
            "reagentBarcode": "NV9999999-ACGTA",
            "v1pre3Id": "666666",
            "dateModified": mock_date_modified,
            "acl": [
                "wid:e4730533-d752-3601-b4b7-8d4d2f6373de"
            ],
            "flowcellBarcode": "BARCODEEE",
            "sampleSheetName": "MockSampleSheet.csv",
            "apiUrl": f"https://api.aps2.sh.basespace.illumina.com/v2/runs/{mock_run_id}",
            "name": mock_instrument_run_id,
            "id": mock_run_id,
            "instrumentRunId": mock_instrument_run_id,
            "status": mock_status
        }

        seq: Sequence = sequence_srv.create_or_update_sequence_from_bssh_event(mock_payload)
        logger.info(seq)
        self.assertIsNotNone(seq)
        self.assertEqual(Sequence.objects.count(), 1)
        self.assertEqual(seq.status, SequenceStatus.STARTED.value)
        self.assertEqual(seq.start_time, mock_date_modified)
        self.assertIsNone(seq.end_time)

    def test_update_sequence_from_bssh_event(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_sequence_srv.SequenceSrvUnitTests.test_update_sequence_from_bssh_event
        """
        mock_seq: Sequence = SequenceFactory()

        mock_run_id = mock_seq.run_id
        mock_instrument_run_id = mock_seq.instrument_run_id
        mock_date_modified = "2023-05-09T22:17:03.1015272Z"
        mock_status = "PendingAnalysis"
        mock_payload = {
            "id": mock_run_id,
            "instrumentRunId": mock_instrument_run_id,
            "dateModified": mock_date_modified,
            "status": mock_status,
        }

        seq: Sequence = sequence_srv.create_or_update_sequence_from_bssh_event(mock_payload)
        logger.info(seq)
        self.assertIsNotNone(seq)
        self.assertEqual(Sequence.objects.count(), 1)
        self.assertEqual(seq.status, SequenceStatus.SUCCEEDED.value)
        self.assertIsNotNone(seq.end_time)
        self.assertEqual(seq.end_time, mock_date_modified)
