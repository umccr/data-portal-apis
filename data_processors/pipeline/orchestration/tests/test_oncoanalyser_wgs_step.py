import json

from django.utils.timezone import now

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import TestConstant, TumorNormalWorkflowFactory, \
    TumorLabMetadataFactory, LabMetadataFactory, TumorLibraryRunFactory, LibraryRunFactory, WtsTumorLabMetadataFactory
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import oncoanalyser_wgs_step
from data_processors.pipeline.services import libraryrun_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class OncoanalyserWgsStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgs_step.OncoanalyserWgsStepUnitTests.test_perform
        """
        self.verify_local()

        tumor_bam = "gds://path/to/tumor.bam"
        normal_bam = "gds://path/to/normal.bam"

        mock_tumor_normal_workflow: Workflow = TumorNormalWorkflowFactory()
        mock_tumor_normal_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_tumor_normal_workflow.end = now()
        mock_tumor_normal_workflow.output = json.dumps({
            "normal_bam_out": {
                "location": normal_bam,
            },
            "tumor_bam_out": {
                "location": tumor_bam,
            }
        })
        mock_tumor_normal_workflow.save()

        mock_meta_wgs_tumor: LabMetadata = TumorLabMetadataFactory()
        mock_meta_wgs_normal: LabMetadata = LabMetadataFactory()
        mock_lbr_tumor: LibraryRun = TumorLibraryRunFactory()
        mock_lbr_normal: LibraryRun = LibraryRunFactory()

        _ = libraryrun_srv.link_library_runs_with_x_seq_workflow(
            library_id_list=[mock_lbr_tumor.library_id, mock_lbr_normal.library_id],
            workflow=mock_tumor_normal_workflow,
        )

        result = oncoanalyser_wgs_step.perform(this_workflow=mock_tumor_normal_workflow)
        self.assertIsNotNone(result)

        logger.info(f"{json.dumps(result)}")
        self.assertEqual(result['subject_id'], TestConstant.subject_id.value)
        self.assertEqual(result['normal_wgs_bam'], normal_bam)
        self.assertEqual(result['tumor_wgs_bam'], tumor_bam)
        self.assertEqual(result['tumor_wgs_library_id'], mock_lbr_tumor.library_id)
        self.assertEqual(result['normal_wgs_library_id'], mock_lbr_normal.library_id)

    def test_fill_ids_from_metadata_differing_subject_error(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgs_step.OncoanalyserWgsStepUnitTests.test_fill_ids_from_metadata_differing_subject_error
        """
        lib1: LabMetadata = TumorLabMetadataFactory()
        lib2: LabMetadata = LabMetadataFactory()
        lib2.subject_id = "SBJ999999"
        lib2.save()

        try:
            _ = oncoanalyser_wgs_step.fill_ids_from_metadata({}, [lib1, lib2])
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_fill_ids_from_metadata_normal_phenotype_error(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgs_step.OncoanalyserWgsStepUnitTests.test_fill_ids_from_metadata_normal_phenotype_error
        """
        lib1: LabMetadata = TumorLabMetadataFactory()
        lib2: LabMetadata = LabMetadataFactory()
        lib2.phenotype = "tumor"
        lib2.save()

        try:
            _ = oncoanalyser_wgs_step.fill_ids_from_metadata({}, [lib1, lib2])
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_fill_ids_from_metadata_more_than_2_error(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_oncoanalyser_wgs_step.OncoanalyserWgsStepUnitTests.test_fill_ids_from_metadata_more_than_2_error
        """
        lib1: LabMetadata = TumorLabMetadataFactory()
        lib2: LabMetadata = LabMetadataFactory()
        lib3: LabMetadata = WtsTumorLabMetadataFactory()

        try:
            _ = oncoanalyser_wgs_step.fill_ids_from_metadata({}, [lib1, lib2, lib3])
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)
