import json
from datetime import datetime

from libiap.openapi import libgds, libwes
from mockito import when

from data_portal.models import Workflow, SequenceRun
from data_portal.tests.factories import SequenceRunFactory, WorkflowFactory
from data_processors.pipeline import workflow
from data_processors.pipeline.dto import WorkflowType, FastQ
from data_processors.pipeline.factory import FastQBuilder, extract_fastq_sample_name
from data_processors.tests import _rand
from data_processors.tests.case import WorkflowCase, logger


class WorkflowTest(WorkflowCase):

    def test_parse_gds_path(self):
        gds_path = "gds://raw-sequence-data-dev/999999_Z99999_0010_AG2CTTAGCT/SampleSheet.csv"
        path_elements = gds_path.replace("gds://", "").split("/")
        logger.info(path_elements)

        run_id = "300101_A99999_0020_AG2CTTAGYY"
        new_gds_path = f"gds://{path_elements[0]}/{run_id}/{path_elements[2]}"
        logger.info(new_gds_path)

        volume_name = path_elements[0]
        path = path_elements[1:]
        logger.info(volume_name)
        logger.info(path)
        logger.info(f"/{'/'.join(path)}/*")
        self.assertTrue(True)

    def test_model_asdict(self):
        spec = workflow.WorkflowSpecification()
        spec.workflow_type = WorkflowType.BCL_CONVERT
        model = workflow.WorkflowDomainModel(spec=spec)
        model.wfr_id = "wfr.SOMETHING_THAT_CAN_BE_MUTATED"
        logger.info(vars(model))
        logger.info(model.asdict())

        logger.info((WorkflowType.BCL_CONVERT, type(WorkflowType.BCL_CONVERT)))
        logger.info((WorkflowType.BCL_CONVERT.name, type(WorkflowType.BCL_CONVERT.name)))
        logger.info((WorkflowType.BCL_CONVERT.value, type(WorkflowType.BCL_CONVERT.value)))

        self.assertFalse("WorkflowType.BCL_CONVERT" == WorkflowType.BCL_CONVERT)
        self.assertTrue("BCL_CONVERT" == WorkflowType.BCL_CONVERT.name)
        self.assertEqual(WorkflowType['BCL_CONVERT'], WorkflowType.BCL_CONVERT)

    def test_fastq_map_build(self):
        workflow = Workflow()
        workflow.wfr_id = f"wfr.{_rand(32)}"
        workflow.type = WorkflowType.BCL_CONVERT
        workflow.output = json.dumps({
            'main/fastqs': {
                'location': f"gds://{workflow.wfr_id}/bclConversion_launch/try-1/out-dir-bclConvert",
                'basename': "out-dir-bclConvert",
                'nameroot': "",
                'nameext': "",
                'class': "Directory",
                'listing': []
            }
        })
        mock_file_list: libgds.FileListResponse = libgds.FileListResponse()
        mock_file_list.items = [
            libgds.FileResponse(name="NA12345 - 4KC_S7_R1_001.fastq.gz"),
            libgds.FileResponse(name="NA12345 - 4KC_S7_R2_001.fastq.gz"),
            libgds.FileResponse(name="PRJ111119_L1900000_S1_R1_001.fastq.gz"),
            libgds.FileResponse(name="PRJ111119_L1900000_S1_R2_001.fastq.gz"),
            libgds.FileResponse(name="MDX199999_L1999999_topup_S2_R1_001.fastq.gz"),
            libgds.FileResponse(name="MDX199999_L1999999_topup_S2_R2_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R1_001.fastq.gz"),
            libgds.FileResponse(name="L9111111_topup_S3_R2_001.fastq.gz"),
        ]
        when(libgds.FilesApi).list_files(...).thenReturn(mock_file_list)

        fastq: FastQ = FastQBuilder(workflow).build()
        for sample_name, bag in fastq.fastq_map.items():
            fastq_list = bag['fastq_list']
            logger.info((sample_name, fastq_list))
        self.assertIsNotNone(fastq)

    def test_fastq_map_build_output_not_json(self):
        workflow = Workflow()
        workflow.wfr_id = f"wfr.{_rand(32)}"
        workflow.type = WorkflowType.BCL_CONVERT
        workflow.output = """
        {
            'main/fastqs': {
                'location': f"gds://{workflow.wfr_id}/bclConversion_launch/try-1/out-dir-bclConvert",
                'basename': "out-dir-bclConvert",
                'nameroot': "",
                'nameext': "",
                'class': "Directory",
                'listing': []
            }
        }
        """
        """
        Should raise similar to:
        
            [ERROR] JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
            Traceback (most recent call last):
              File "/var/task/data_processors/lambdas/iap.py", line 105, in handler
                next_model.launch()
              File "/var/task/data_processors/pipeline/workflow.py", line 143, in launch
                fastq: FastQ = FastQBuilder(parent).build()
              File "/var/task/data_processors/pipeline/factory.py", line 39, in build
                output_gds_path: str = json.loads(fastq_output)['main/fastqs']['location']
              File "/var/lang/lib/python3.8/json/__init__.py", line 357, in loads
                return _default_decoder.decode(s)
              File "/var/lang/lib/python3.8/json/decoder.py", line 337, in decode
                obj, end = self.raw_decode(s, idx=_w(s, 0).end())
              File "/var/lang/lib/python3.8/json/decoder.py", line 353, in raw_decode
                obj, end = self.scan_once(s, idx)
        
        Storing models.Workflow.output into database should always be in JSON format. 
        Fixed with this PR https://github.com/umccr/data-portal-apis/pull/90
        """
        try:
            FastQBuilder(workflow).build()
        except Exception as e:
            logger.exception(f"THIS 'json.decoder.JSONDecodeError' IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_workflow_output_format(self):
        config = libwes.Configuration(
            host="http://localhost",
            api_key={
                'Authorization': "mock"
            },
            api_key_prefix={
                'Authorization': "Bearer"
            },
        )
        with libwes.ApiClient(config) as api_client:
            run_api = libwes.WorkflowRunsApi(api_client)
            wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id="anything_work")
            logger.info((wfl_run.output, type(wfl_run.output)))
            logger.info((wfl_run.time_stopped, type(wfl_run.time_stopped)))
            logger.info((wfl_run.status, type(wfl_run.status)))

        self.assertTrue(isinstance(wfl_run.output, dict))
        self.assertTrue(isinstance(wfl_run.time_stopped, datetime))
        self.assertTrue(isinstance(wfl_run.status, str))

    def test_fastq_extract_sample_name(self):
        filenames = [
            "NA12345 - 4KC_S7_L001_R1_001.fastq.gz",
            "NA12345 - 4KC_S7_L001_R2_001.fastq.gz",
            "NA12345 - 4KC_S7_L002_R1_001.fastq.gz",
            "NA12345 - 4KC_S7_L002_R2_001.fastq.gz",
            "L2000552_S1_R1_001.fastq.gz",
            "L2000552_S1_R2_001.fastq.gz",
            "L1000555_S3_R1_001.fastq.gz",
            "L1000555_S3_R2_001.fastq.gz",
            "L1000555_S3_R3_001.fastq.gz",
            "L3000666_S7_R1_001.fastq.gz",
            "L4000888_S99_R1_001.fastq.gz",
            "L4000888_S3K_S99_R2_001.fastq.gz",
            "L4000888_SK_S99_I1_001.fastq.gz",
            "L400S888_S99_I2_001.fastq.gz",
            "L400S888_S5-9_S99_I2_001.fastq.gz",
            "PTC_TsqN999999_L9900001_S101_I2_001.fastq.gz",
            "PRJ111119_L1900000_S102_I2_001.fastq.gz",
            "MDX199999_L1999999_topup_S201_I2_001.fastq.gz",
        ]

        for name in filenames:
            sample_name = extract_fastq_sample_name(name)
            logger.info((sample_name, name))
            self.assertTrue("_R" not in sample_name)

        self.assertIsNone(extract_fastq_sample_name("L1999999_topup_R1_001.fastq.gz"))

    def test_bcl_convert(self):
        mock_sqr: SequenceRun = SequenceRunFactory()

        spec = workflow.WorkflowSpecification()
        spec.sequence_run = mock_sqr
        spec.workflow_type = WorkflowType.BCL_CONVERT
        workflow.WorkflowDomainModel(spec).launch()

        # assert bcl convert workflow launch success and save workflow runs in portal db
        success_bcl_convert_workflow_runs = Workflow.objects.all()
        self.assertEqual(1, success_bcl_convert_workflow_runs.count())

    def test_germline(self):
        self.verify()
        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_fastq: FastQ = FastQ()
        mock_fastq.volume_name = f"{mock_bcl_workflow.wfr_id}"
        mock_fastq.path = f"/bclConversion_launch/try-1/out-dir-bclConvert"
        mock_fastq.gds_path = f"gds://{mock_fastq.volume_name}{mock_fastq.path}"
        mock_fastq.fastq_map = {
            'SAMPLE_ACGT1': {
                'fastq_list': ['SAMPLE_ACGT1_S1_L001_R1_001.fastq.gz', 'SAMPLE_ACGT1_S1_L001_R2_001.fastq.gz'],
                'tags': ['SBJ00001'],
            },
            'SAMPLE_ACGT2': {
                'fastq_list': ['SAMPLE_ACGT2_S2_L001_R1_001.fastq.gz', 'SAMPLE_ACGT2_S2_L001_R2_001.fastq.gz'],
                'tags': ['SBJ00001'],
            },
        }
        when(workflow.FastQBuilder).build().thenReturn(mock_fastq)

        # scenario: bcl convert bssh.runs event "complete" arrive
        # assume both sequence run and bcl convert were success and persisted in portal db
        parent = Workflow.objects.get(wfr_id=mock_bcl_workflow.wfr_id)

        # so that we can launch germline workflow
        spec = workflow.WorkflowSpecification()
        spec.sequence_run = mock_bcl_workflow.sequence_run
        spec.parents = [parent]
        spec.workflow_type = WorkflowType.GERMLINE
        workflow.WorkflowDomainModel(spec).launch()

        # assert germline workflow launch success and save workflow runs in portal db
        success_germline_workflow_runs = Workflow.objects.all()
        self.assertEqual(3, success_germline_workflow_runs.count())
