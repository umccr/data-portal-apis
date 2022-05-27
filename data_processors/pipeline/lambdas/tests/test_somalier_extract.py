from unittest import skip

from data_processors.lims.lambdas import labmetadata
from data_processors.pipeline.lambdas import somalier_extract
from data_processors.pipeline.tests.case import logger, PipelineIntegrationTestCase


class SomalierExtractIntegrationTests(PipelineIntegrationTestCase):
    # integration test cases are meant to hit IAP/AWS endpoints

    @skip
    def test_somalier_extract_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_somalier_extract.SomalierExtractIntegrationTests.test_somalier_extract_handler
        """

        # Create extraction event
        event = {
            # Output from wfr.0d3dea278b1c471d8316b9d5a242dd34
            "gds_path": "gds://development/analysis_data/SBJ00913/wgs_alignment_qc/20220312c26574d6/L2100747__2_dragen/MDX210178.bam"
        }

        somalier_extract_handler: dict = somalier_extract.handler(event, None)

        self.assertIsNotNone(somalier_extract_handler)
        self.assertIsInstance(somalier_extract_handler, dict)
        self.assertIsNotNone(somalier_extract_handler.get("executionArn", None))

