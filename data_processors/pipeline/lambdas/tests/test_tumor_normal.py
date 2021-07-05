import json

from data_portal.models import Workflow
from data_processors.pipeline.lambdas import tumor_normal
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class TumorNormalUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_tumor_normal.TumorNormalUnitTests.test_handler
        """

        workflow: dict = tumor_normal.handler({
            "subject_id": "SUBJECT_ID",
            "sample_name": "SAMPLE_NAME",
            "fastq_list_rows": [{
                "rgid": "index1.index2.lane",
                "rgsm": "sample_name",
                "rglb": "UnknownLibrary",
                "lane": 1,
                "read_1": {
                    "class": "File",
                    "location": "gds://path/to/read_1.fastq.gz"
                },
                "read_2": {
                    "class": "File",
                    "location": "gds://path/to/read_2.fastq.gz"
                }
            }],
            "tumor_fastq_list_rows": [{
                "rgid": "index1.index2.lane",
                "rgsm": "sample_name",
                "rglb": "UnknownLibrary",
                "lane": 1,
                "read_1": {
                    "class": "File",
                    "location": "gds://path/to/read_1.fastq.gz"
                },
                "read_2": {
                    "class": "File",
                    "location": "gds://path/to/read_2.fastq.gz"
                }
            }],
            "output_file_prefix": "SAMPLEID_LIBRARYID",
            "output_directory": "SAMPLEID_LIBRARYID"
        }, None)

        logger.info("-" * 32)
        logger.info("Example tumor_normal.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert tumor_normal workflow launch success and save workflow run in db
        workflows = Workflow.objects.all()
        self.assertEqual(1, workflows.count())
