from unittest import skip

from data_processors.lims.lambdas import labmetadata
from data_processors.pipeline.lambdas import somalier_check
from data_processors.pipeline.tests.case import logger, PipelineIntegrationTestCase


class SomalierCheckIntegrationTests(PipelineIntegrationTestCase):
    # integration test cases are meant to hit IAP/AWS endpoints

    @skip
    def test_somalier_check_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_somalier_check.SomalierCheckIntegrationTests.test_somalier_check_handler
        """

        # Create check event
        event = {
            # Output from wfr.0d3dea278b1c471d8316b9d5a242dd34
            "index": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam"
        }

        somalier_check_handler: list = somalier_check.handler(event, None)
        self.assertIsNotNone(somalier_check_handler)
        self.assertIsInstance(somalier_check_handler, list)
        # Assert that the list returned is at least itself
        self.assertGreaterEqual(len(somalier_check_handler), 1)
