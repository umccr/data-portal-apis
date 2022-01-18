import json

from data_portal.models.workflow import Workflow
from data_portal.models.libraryrun import LibraryRun
from data_portal.tests.factories import TestConstant, LibraryRunFactory, TumorLibraryRunFactory
from data_processors.pipeline.lambdas import umccrise
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase


class UmccriseLambdaUnitTests(PipelineUnitTestCase):

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_umccrise.UmccriseLambdaUnitTests.test_handler
        """
        mock_normal_library_run: LibraryRun = LibraryRunFactory()
        mock_tumor_library_run: LibraryRun = TumorLibraryRunFactory()

        workflow: dict = umccrise.handler({
            "dragen_somatic_directory": {
                "class": "Directory",
                "location": "gds://path/to/somatic/output/dir"
            },
            "fastq_list_rows_germline": [{
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
            "output_directory_germline": "PRJ1234567",
            "output_directory_umccrise": "TumorRglb__NormalRglb",
            "output_file_prefix_germline": "PRJ1234567",
            "subject_identifier_umccrise": "SBJ01234",
            "sample_name": "TUMOR_SAMPLE_ID",
            "tumor_library_id": f"{TestConstant.library_id_tumor.value}",
            "normal_library_id": f"{TestConstant.library_id_normal.value}"
        }, None)

        logger.info("-" * 32)
        logger.info("Example umccrise.handler lambda output:")
        logger.info(json.dumps(workflow))

        # assert umccrise workflow launch success and save workflow run in db
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
