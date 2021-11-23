from data_portal.models.sequencerun import SequenceRun
from data_portal.tests.factories import TestConstant, SequenceRunFactory
from data_processors.pipeline.services import sequencerun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class SequenceRunSrvUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(SequenceRunSrvUnitTests, self).setUp()

    def test_create_or_update_sequence_run(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_sequencerun_srv.SequenceRunSrvUnitTests.test_create_or_update_sequence_run
        """
        mock_payload = {
            'id': TestConstant.run_id.value,
            'instrumentRunId': TestConstant.instrument_run_id.value,
            'dateModified': "2020-05-09T22:17:10.815Z",
            'status': "New",
            'gdsFolderPath': "",
            'gdsVolumeName': "",
            'reagentBarcode': "",
            'v1pre3Id': "",
            'acl': "",
            'flowcellBarcode': "",
            'sampleSheetName': "",
            'apiUrl': "",
            'name': TestConstant.instrument_run_id.value,
            'messageAttributesAction': "",
            'messageAttributesActionDate': "2020-05-09T22:17:10.815Z",
            'messageAttributesActionType': "",
            'messageAttributesProducedBy': "",
        }
        sqr = sequencerun_srv.create_or_update_sequence_run(mock_payload)
        self.assertIsNotNone(sqr)
        sqr_in_db = SequenceRun.objects.get(instrument_run_id=TestConstant.instrument_run_id.value)
        self.assertEqual(sqr.id, sqr_in_db.id)

    def test_create_or_update_sequence_run_ignored(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_sequencerun_srv.SequenceRunSrvUnitTests.test_create_or_update_sequence_run_ignored
        """
        mock_sqr: SequenceRun = SequenceRunFactory()
        sqr = sequencerun_srv.create_or_update_sequence_run({
            'id': mock_sqr.run_id,
            'instrumentRunId': mock_sqr.instrument_run_id,
            'dateModified': mock_sqr.date_modified,
            'status': mock_sqr.status,
        })
        self.assertIsNone(sqr)
        self.assertEqual(1, SequenceRun.objects.count())

    def test_get_sequence_run_by_run_id(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_sequencerun_srv.SequenceRunSrvUnitTests.test_get_sequence_run_by_run_id
        """
        _ = SequenceRunFactory()
        sqr = sequencerun_srv.get_sequence_run_by_run_id(TestConstant.run_id.value)
        self.assertIsNotNone(sqr)
        logger.info(sqr)
        self.assertEqual("PendingAnalysis", sqr.status)

    def test_get_sequence_run_by_instrument_run_ids(self):
        """
        python manage.py test data_processors.pipeline.services.tests.test_sequencerun_srv.SequenceRunSrvUnitTests.test_get_sequence_run_by_instrument_run_ids
        """
        _ = SequenceRunFactory()
        sqr_list = sequencerun_srv.get_sequence_run_by_instrument_run_ids([TestConstant.instrument_run_id.value])
        self.assertEqual(1, len(sqr_list))
