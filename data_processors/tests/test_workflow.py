import json
import os

from django.test import TestCase
from mockito import when, unstub

from data_portal.models import Workflow, SequenceRun
from data_portal.tests.factories import SequenceRunFactory, WorkflowFactory
from data_processors.pipeline import workflow
from data_processors.pipeline.dto import WorkflowType, FastQ
from data_processors.pipeline.factory import FastQBuilder, extract_sample_id
from data_processors.tests import _rand


class WorkflowTest(TestCase):

    def setUp(self) -> None:
        os.environ['IAP_BASE_URL'] = "http://localhost"
        os.environ['IAP_AUTH_TOKEN'] = "mock"
        os.environ['IAP_WES_WORKFLOW_ID'] = WorkflowFactory.wfl_id
        os.environ['IAP_WES_WORKFLOW_VERSION_NAME'] = WorkflowFactory.version

    def tearDown(self) -> None:
        del os.environ['IAP_BASE_URL']
        del os.environ['IAP_AUTH_TOKEN']
        del os.environ['IAP_WES_WORKFLOW_ID']
        del os.environ['IAP_WES_WORKFLOW_VERSION_NAME']
        unstub()

    def test_parse_gds_path(self):
        gds_path = "gds://raw-sequence-data-dev/999999_Z99999_0010_AG2CTTAGCT/SampleSheet.csv"
        path_elements = gds_path.replace("gds://", "").split("/")
        print(path_elements)
        run_id = "300101_A99999_0020_AG2CTTAGYY"
        new_gds_path = f"gds://{path_elements[0]}/{run_id}/{path_elements[2]}"
        print(new_gds_path)

        volume_name = path_elements[0]
        path = path_elements[1:]
        print(volume_name)
        print(path)
        print(f"/{'/'.join(path)}/*")
        self.assertTrue(True)

    def test_model_asdict(self):
        spec = workflow.WorkflowSpecification()
        spec.workflow_type = WorkflowType.BCL_CONVERT
        model = workflow.WorkflowDomainModel(spec=spec)
        model.wfr_id = "wfr.SOMETHING_MOCK"
        print(vars(model))
        print()
        print(model.asdict())
        print(WorkflowType.BCL_CONVERT)
        print(type(WorkflowType.BCL_CONVERT))
        print(WorkflowType.BCL_CONVERT.name)
        print(type(WorkflowType.BCL_CONVERT.name))
        print(WorkflowType.BCL_CONVERT.value)
        print(type(WorkflowType.BCL_CONVERT.value))
        print("WorkflowType.BCL_CONVERT" == WorkflowType.BCL_CONVERT)
        print("BCL_CONVERT" == WorkflowType.BCL_CONVERT.name)
        print(WorkflowType['BCL_CONVERT'])
        self.assertTrue(True)

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
        fastq: FastQ = FastQBuilder(workflow).build()
        print(fastq)
        self.assertTrue(True)

    def test_fastq_extract_sample_id(self):
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
            "L4000888_S99_R2_001.fastq.gz",
            "L4000888_S99_I1_001.fastq.gz",
            "L4000888_S99_I2_001.fastq.gz",
        ]

        for name in filenames:
            sample_id = extract_sample_id(name)
            print(sample_id)

        self.assertTrue(True)

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
