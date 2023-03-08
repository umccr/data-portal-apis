import json
from typing import List, Dict
from unittest import skip

from libica.app import wes
from libumccr import libjson

from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType, LabMetadataWorkflow
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TumorNormalWorkflowFactory, TestConstant
from data_processors.pipeline.orchestration import umccrise_step
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger

mock_tn_subject_id = "SBJ00001"

mock_tn_output = json.dumps({
    "dragen_somatic_output_directory": {
        "basename": f"{TestConstant.library_id_tumor.value}_{TestConstant.library_id_normal.value}_dragen_somatic",
        "class": "Directory",
        "location": f"gds://vol/analysis_data/{mock_tn_subject_id}/wgs_tumor_normal/20310102aa4f9099/{TestConstant.library_id_tumor.value}_{TestConstant.library_id_normal.value}_dragen_somatic",
        "nameext": "",
        "nameroot": "",
        "size": None
    },
    "dragen_germline_output_directory": {
        "basename": f"{TestConstant.library_id_normal.value}_dragen_germline",
        "class": "Directory",
        "location": f"gds://vol/analysis_data/{mock_tn_subject_id}/wgs_tumor_normal/20310102aa4f9099/{TestConstant.library_id_normal.value}_dragen_germline",
        "nameext": "",
        "nameroot": "",
        "size": None
    }
})

mock_tn_input = json.dumps({
    'fastq_list_rows': [
        {
            "rgid": f"GCAGAATT.TGGCCGGT.4.310101_A00130_0001_BHWLGFDSX2.PRJ310196_{TestConstant.library_id_normal.value}",
            "rglb": f"{TestConstant.library_id_normal.value}",
            "rgsm": "PRJ310196",
            "lane": 4,
            "read_1": {
                "class": "File",
                "location": "gds://vol/primary_data/310101_A00130_0001_BHWLGFDSX2/20310101a1cdffe3/WGS_TsqNano/PRJ310196_L3101001_S7_L004_R1_001.fastq.gz"
            },
            "read_2": {
                "class": "File",
                "location": "gds://vol/primary_data/310101_A00130_0001_BHWLGFDSX2/20310101a1cdffe3/WGS_TsqNano/PRJ310196_L3101001_S7_L004_R2_001.fastq.gz"
            }
        },
    ],
    'tumor_fastq_list_rows': [
        {
            "rgid": f"ATGAGGCC.CAATTAAC.4.310101_A00130_0001_BHWLGFDSX2.PRJ310197_{TestConstant.library_id_tumor.value}",
            "rglb": f"{TestConstant.library_id_tumor.value}",
            "rgsm": f"{TestConstant.sample_id.value}",
            "lane": 4,
            "read_1": {
                "class": "File",
                "location": "gds://vol/primary_data/310101_A00130_0001_BHWLGFDSX2/20310101a1cdffe3/WGS_TsqNano/PRJ310197_L3101002_S8_L004_R1_001.fastq.gz"
            },
            "read_2": {
                "class": "File",
                "location": "gds://vol/primary_data/310101_A00130_0001_BHWLGFDSX2/20310101a1cdffe3/WGS_TsqNano/PRJ310197_L3101002_S8_L004_R2_001.fastq.gz"
            }
        },
    ],
})


def build_mock():
    mock_labmetadata_normal = LabMetadata()
    mock_labmetadata_normal.subject_id = mock_tn_subject_id
    mock_labmetadata_normal.library_id = TestConstant.library_id_normal.value
    mock_labmetadata_normal.phenotype = LabMetadataPhenotype.NORMAL.value
    mock_labmetadata_normal.type = LabMetadataType.WGS.value
    mock_labmetadata_normal.workflow = LabMetadataWorkflow.CLINICAL.value
    mock_labmetadata_normal.save()

    mock_labmetadata_tumor = LabMetadata()
    mock_labmetadata_tumor.subject_id = mock_tn_subject_id
    mock_labmetadata_tumor.library_id = TestConstant.library_id_tumor.value
    mock_labmetadata_tumor.sample_id = TestConstant.sample_id.value
    mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
    mock_labmetadata_tumor.type = LabMetadataType.WGS.value
    mock_labmetadata_tumor.workflow = LabMetadataWorkflow.CLINICAL.value
    mock_labmetadata_tumor.save()


class UmccriseStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_perform
        """
        mock_tn_workflow: Workflow = TumorNormalWorkflowFactory()
        mock_tn_workflow.input = mock_tn_input
        mock_tn_workflow.output = mock_tn_output
        mock_tn_workflow.save()

        build_mock()

        results = umccrise_step.perform(this_workflow=mock_tn_workflow)
        self.assertIsNotNone(results)

        logger.info(f"{json.dumps(results)}")
        self.assertEqual(results['submitting_subjects'][0], mock_tn_subject_id)

    def test_prepare_umccrise_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_prepare_umccrise_jobs
        """
        mock_tn_workflow: Workflow = TumorNormalWorkflowFactory()
        mock_tn_workflow.input = mock_tn_input
        mock_tn_workflow.output = mock_tn_output
        mock_tn_workflow.save()

        build_mock()

        job_list: List[Dict] = umccrise_step.prepare_umccrise_jobs(mock_tn_workflow)
        self.assertIsNotNone(job_list)

        for job in job_list:
            logger.info(f"\n{libjson.dumps(job)}")  # NOTE libjson is intentional and part of ser/deser test
            self.assertIn("output_directory_name", job.keys())

    def test_get_fastq_list_rows_from_somatic_workflow_input(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_get_fastq_list_rows_from_somatic_workflow_input
        """

        fqlr_germline, fqlr_tumor = umccrise_step.get_fastq_list_rows_from_somatic_workflow_input(mock_tn_input)

        self.assertIsNotNone(fqlr_germline)
        self.assertIsNotNone(fqlr_tumor)
        self.assertEqual(fqlr_germline[0]['rglb'], TestConstant.library_id_normal.value)
        self.assertEqual(fqlr_tumor[0]['rglb'], TestConstant.library_id_tumor.value)

    def test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_empty(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_empty
        """
        mock_somatic_input = json.dumps({
            'fastq_list_rows': [],
            'tumor_fastq_list_rows': [],
        })

        try:
            _, _ = umccrise_step.get_fastq_list_rows_from_somatic_workflow_input(mock_somatic_input)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_empty_tumor(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_empty_tumor
        """
        mock_somatic_input = json.dumps({
            'fastq_list_rows': [{'does': "not matter"}],
            'tumor_fastq_list_rows': [],
        })

        try:
            _, _ = umccrise_step.get_fastq_list_rows_from_somatic_workflow_input(mock_somatic_input)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_none(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_none
        """
        mock_somatic_input = json.dumps({
            'fastq_list_rows': None,
            'tumor_fastq_list_rows': [],
        })

        try:
            _, _ = umccrise_step.get_fastq_list_rows_from_somatic_workflow_input(mock_somatic_input)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_none_tumor(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepUnitTests.test_get_fastq_list_rows_from_somatic_workflow_input_fqlr_none_tumor
        """
        mock_somatic_input = json.dumps({
            'fastq_list_rows': [{'does': "not matter"}],
        })

        try:
            _, _ = umccrise_step.get_fastq_list_rows_from_somatic_workflow_input(mock_somatic_input)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)


class UmccriseStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_umccrise_jobs(self):
        """
        unset ICA_ACCESS_TOKEN
        export AWS_PROFILE=dev
        python manage.py test data_processors.pipeline.orchestration.tests.test_umccrise_step.UmccriseStepIntegrationTests.test_prepare_umccrise_jobs
        """

        # a somatic+germline workflow run from DEV
        somatic_workflow_id = "wfr.5616c72c82e442f78f0f9f0d6441219e"

        #  ---

        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2021"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        mock_somatic_workflow: Workflow = TumorNormalWorkflowFactory()

        tumor_normal_run = wes.get_run(somatic_workflow_id, to_dict=True)
        mock_somatic_workflow.wfr_id = somatic_workflow_id
        mock_somatic_workflow.input = json.dumps(tumor_normal_run['input'])
        mock_somatic_workflow.output = json.dumps(tumor_normal_run['output'])
        mock_somatic_workflow.save()

        job_list = umccrise_step.prepare_umccrise_jobs(this_workflow=mock_somatic_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)
