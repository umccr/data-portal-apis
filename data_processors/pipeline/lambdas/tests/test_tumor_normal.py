import json

from data_portal.models import Workflow, LibraryRun
from data_portal.tests.factories import TestConstant, LibraryRunFactory, TumorLibraryRunFactory
from data_processors.pipeline.lambdas import tumor_normal
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class TumorNormalUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_tumor_normal.TumorNormalUnitTests.test_handler
        """
        mock_normal_library_run: LibraryRun = LibraryRunFactory()
        mock_tumor_library_run: LibraryRun = TumorLibraryRunFactory()

        workflow: dict = tumor_normal.handler({
            "subject_id": "SUBJECT_ID",
            "sample_name": "SAMPLE_NAME",
            "fastq_list_rows": [{
                "rgid": "index1.index2.lane",
                "rgsm": "sample_name",
                "rglb": TestConstant.library_id_normal.value,
                "lane": TestConstant.lane_normal_library.value,
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
                "rglb": TestConstant.library_id_tumor.value,
                "lane": TestConstant.lane_tumor_library.value,
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
        qs = Workflow.objects.all()
        self.assertEqual(1, qs.count())

        # assert that we can query related LibraryRun from Workflow side
        wfl = qs.get()
        all_lib_runs = workflow_srv.get_all_library_runs_by_workflow(wfl)
        self.assertEqual(2, len(all_lib_runs))
        fixtures = [TestConstant.library_id_normal.value, TestConstant.library_id_tumor.value]
        for lib_run in all_lib_runs:
            logger.info(lib_run)
            self.assertIn(lib_run.library_id, fixtures)
