import logging

from django.db import IntegrityError
from django.test import TestCase

from data_portal.models.sequencerun import SequenceRun
from data_portal.tests.factories import SequenceRunFactory

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SequenceRunTests(TestCase):

    def test_save_sequence_run(self):
        sequence_run: SequenceRun = SequenceRunFactory()
        logger.info(sequence_run)
        self.assertEqual(1, SequenceRun.objects.count())
        self.assertEqual(sequence_run.name, SequenceRun.objects.get(run_id=sequence_run.run_id).name)

    def test_save_duplicate_sequence_run(self):
        sequence_run: SequenceRun = SequenceRunFactory()
        logger.info(f"Created first SequenceRun record. {sequence_run}")
        self.assertEqual(1, SequenceRun.objects.count())
        try:
            logger.info(f"Attempt to create another SequenceRun with the same unique composite key")
            sequence_run_copycat: SequenceRun = SequenceRunFactory()
        except IntegrityError as e:
            logger.info(f"Raised IntegrityError: {e}")
        self.assertRaises(IntegrityError)
