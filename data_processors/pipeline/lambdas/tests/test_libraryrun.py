from unittest import skip

from data_portal.models.libraryrun import LibraryRun
from data_processors.lims.lambdas import labmetadata
from data_processors.pipeline.lambdas import libraryrun
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger


class LibraryRunUnitTests(PipelineUnitTestCase):
    pass


class LibraryRunIntegrationTests(PipelineIntegrationTestCase):

    @skip
    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_libraryrun.LibraryRunIntegrationTests.test_handler
        """

        # populate test db with LabMetadata
        stat = labmetadata.scheduled_update_handler({'event': "LibraryRunSrvIntegrationTests", 'truncate': False}, None)
        logger.info(f"{stat}")

        # SEQ-II validation dataset
        gds_volume_name = "umccr-raw-sequence-data-dev"
        gds_folder_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg"

        payload = {
            'gds_volume_name': gds_volume_name,
            'gds_folder_path': gds_folder_path,
            'instrument_run_id': '200612_A01052_0017_BH5LYWDSXY',
            'run_id': 'r.Uvlx2DEIME-KH0BRyF9XBg',
        }

        results = libraryrun.handler(payload, None)

        logger.info("-" * 32)
        logger.info(results)

        self.assertTrue(len(results) > 0)
        self.assertEqual(len(results), LibraryRun.objects.count())
