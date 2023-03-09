import json
from datetime import datetime
from typing import List, Dict
from unittest import skip

from django.utils.timezone import now, make_aware
from libica.app import wes
from libumccr import libjson
from mockito import when

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import UmccriseWorkflowFactory, LibraryRunFactory, \
    TumorLibraryRunFactory, LabMetadataFactory, TumorLabMetadataFactory, DragenWtsWorkflowFactory, \
    WtsTumorLabMetadataFactory, WtsTumorLibraryRunFactory, TestConstant, DragenWtsWorkflowFactory2, SequenceRunFactory, \
    SequenceRunFactory2
from data_processors.pipeline.domain.workflow import WorkflowStatus
from data_processors.pipeline.orchestration import rnasum_step
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase, logger

mock_umccrise_output = json.dumps({
    "umccrise_output_directory": {
        "location": "gds://vol/analysis_data/SBJ00001/umccrise/2022012324bd4c96/L3200003__L3200004",
        "basename": "L3200003__L3200004",
        "nameroot": "",
        "nameext": "",
        "class": "Directory",
        "size": None
    },
    "output_dir_gds_session_id": "ssn.611111111e9c400aa6aa3652951d91a8",
    "output_dir_gds_folder_id": "fol.ccccccc6ca06666666d008d89d4636ab"
})

mock_wts_tumor_only_output = json.dumps({
    "arriba_output_directory": {
        "location": "gds://vol/analysis_data/SBJ00001/wts_tumor_only/202201232df89fee/arriba_outputs",
        "basename": "arriba_outputs",
        "nameroot": "",
        "nameext": "",
        "class": "Directory",
        "size": None
    },
    "dragen_transcriptome_output_directory": {
        "location": "gds://vol/analysis_data/SBJ00001/wts_tumor_only/202201232df89fee/L3200000_dragen",
        "basename": "L3200000_dragen",
        "nameroot": "",
        "nameext": "",
        "class": "Directory",
        "size": None
    },
    "multiqc_output_directory": {
        "location": "gds://vol/analysis_data/SBJ00001/wts_tumor_only/202201232df89fee/dragen_transcriptome_multiqc",
        "basename": "dragen_transcriptome_multiqc",
        "nameroot": "",
        "nameext": "",
        "class": "Directory",
        "size": None
    },
    "output_dir_gds_session_id": "ssn.99999999b45b4f96bc9baf056a79ede2",
    "output_dir_gds_folder_id": "fol.cccccccca064e0362d008d89d4636ab"
})


def build_mock():
    mock_normal_wgs_library_run = LibraryRunFactory()
    mock_tumor_wgs_library_run = TumorLibraryRunFactory()
    mock_tumor_wts_library_run = WtsTumorLibraryRunFactory()

    mock_normal_wgs_library_meta = LabMetadataFactory()
    mock_tumor_wgs_library_meta = TumorLabMetadataFactory()
    mock_tumor_wts_library_meta: LabMetadata = WtsTumorLabMetadataFactory()

    mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
    mock_umccrise_workflow.output = mock_umccrise_output
    mock_umccrise_workflow.libraryrun_set.add(mock_normal_wgs_library_run)
    mock_umccrise_workflow.libraryrun_set.add(mock_tumor_wgs_library_run)
    mock_umccrise_workflow.save()

    mock_wts_workflow: Workflow = DragenWtsWorkflowFactory()
    mock_wts_workflow.libraryrun_set.add(mock_tumor_wts_library_run)
    mock_wts_workflow.end_status = WorkflowStatus.SUCCEEDED.value
    mock_wts_workflow.end = now()
    mock_wts_workflow.output = mock_wts_tumor_only_output
    mock_wts_workflow.save()

    when(rnasum_step).lookup_tcga_dataset(...).thenReturn("THCA")

    return mock_umccrise_workflow


class RNAsumStepUnitTests(PipelineUnitTestCase):

    def test_perform(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepUnitTests.test_perform
        """
        mock_umccrise_workflow = build_mock()

        results = rnasum_step.perform(mock_umccrise_workflow)
        self.assertIsNotNone(results)

        logger.info(f"{json.dumps(results)}")
        self.assertEqual(results['submitting_subjects'][0], TestConstant.subject_id.value)

    def test_prepare_rnasum_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepUnitTests.test_prepare_rnasum_jobs
        """
        mock_umccrise_workflow = build_mock()

        job_list: List[Dict] = rnasum_step.prepare_rnasum_jobs(mock_umccrise_workflow)
        self.assertIsNotNone(job_list)

        for job in job_list:
            logger.info(f"\n{libjson.dumps(job)}")  # NOTE libjson is intentional and part of ser/deser test
            self.assertIn("dragen_transcriptome_directory", job.keys())
            self.assertIn("umccrise_directory", job.keys())
            self.assertEqual(job['subject_id'], TestConstant.subject_id.value)
            self.assertEqual(job['tumor_library_id'], TestConstant.wts_library_id_tumor.value)

    def test_deduce_umccrise_result_location_from_root_dir(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepUnitTests.test_deduce_umccrise_result_location_from_root_dir
        """
        mock_umccrise_directory = {
            "location": "gds://production/analysis_data/SBJ00000/umccrise/20220327e8ba9649/L3200000__L3200001",
            "basename": "L3200000__L3200001",
            "nameroot": "L3200000__L3200001",
            "nameext": "",
            "class": "Directory",
            "size": None
        }

        rnasum_step.deduce_umccrise_result_location_from_root_dir(mock_umccrise_directory, "SBJ00000", "MDX320001")

        logger.info(mock_umccrise_directory['location'])
        self.assertIn("MDX320001", mock_umccrise_directory['location'])

    def test_lookup_tcga_dataset(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepUnitTests.test_lookup_tcga_dataset
        """
        pass


class RNAsumStepIntegrationTests(PipelineIntegrationTestCase):

    @skip
    def test_prepare_rnasum_jobs_SBJ01670(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_prepare_rnasum_jobs_SBJ01670
        """

        # Replaying https://data.umccr.org/subjects/SBJ01670
        # awscurl --profile prodops --region ap-southeast-2 -H "Accept: application/json" "https://api.portal.prod.umccr.org/iam/libraryrun?library_id=L2200320" | jq

        wgs_library_normal = LibraryRun.objects.create(
            library_id="L2200319",
            instrument_run_id="220311_A01052_0085_AHGGTWDSX3",
            run_id="r.85",
            lane=3,
            override_cycles="Y151;I8;I8;Y151",
        )

        wgs_library_tumor = LibraryRun.objects.create(
            library_id="L2200320",
            instrument_run_id="220311_A01052_0085_AHGGTWDSX3",
            run_id="r.85",
            lane=3,
            override_cycles="Y151;I8;I8;Y151",
        )

        wgs_library_tumor_topup = LibraryRun.objects.create(
            library_id="L2200320",
            instrument_run_id="220325_A01052_0086_AHGGCKDSX3",
            run_id="r.86",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor = LibraryRun.objects.create(
            library_id="L2200308",
            instrument_run_id="220311_A01052_0084_BH3TYCDSX3",
            run_id="r.84",
            lane=4,
            override_cycles="Y151;I8N2;I8N2;Y151",
        )

        umccrise_workflow_id = "wfr.f5b92a35bcf6418c9420a432cd013f71"   # prod
        wts_workflow_id = "wfr.d0399c0d6c4341029733d70dd1484805"        # prod

        #  ---

        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2021", "2022"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
        mock_wts_workflow: Workflow = DragenWtsWorkflowFactory()

        umccrise_wfl_run = wes.get_run(umccrise_workflow_id, to_dict=True)
        mock_umccrise_workflow.wfr_id = umccrise_workflow_id
        mock_umccrise_workflow.input = json.dumps(umccrise_wfl_run['input'])
        mock_umccrise_workflow.output = json.dumps(umccrise_wfl_run['output'])
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_normal)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor_topup)
        mock_umccrise_workflow.end = now()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        wts_wfl_run = wes.get_run(wts_workflow_id, to_dict=True)
        mock_wts_workflow.wfr_id = wts_workflow_id
        mock_wts_workflow.input = json.dumps(wts_wfl_run['input'])
        mock_wts_workflow.output = json.dumps(wts_wfl_run['output'])
        mock_wts_workflow.libraryrun_set.add(wts_library_tumor)
        mock_wts_workflow.end = now()
        mock_wts_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow.save()

        job_list = rnasum_step.prepare_rnasum_jobs(this_workflow=mock_umccrise_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)

        # Extra assert that we have really pointed to umccrise sample result subdirectory
        wgs_tumor_sample_id = LabMetadata.objects.get(library_id__iexact="L2200320").sample_id
        self.assertIn(wgs_tumor_sample_id, job_list[0]['umccrise_directory']['location'])

    @skip
    def test_prepare_rnasum_jobs_SBJ01285(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_prepare_rnasum_jobs_SBJ01285
        """

        # Replaying https://data.umccr.org/subjects/SBJ01285
        # curl -s -X GET -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/libraryrun?library_id=L2101732&instrument_run_id=220121_A00130_0197_BHWJ2HDSX2" | jq

        wgs_library_normal = LibraryRun.objects.create(
            library_id="L2101731",
            instrument_run_id="220121_A00130_0197_BHWJ2HDSX2",
            run_id="r.197",
            lane=2,
            override_cycles="Y151;I8;I8;Y151",
        )

        wgs_library_tumor = LibraryRun.objects.create(
            library_id="L2101732",
            instrument_run_id="220121_A00130_0197_BHWJ2HDSX2",
            run_id="r.197",
            lane=2,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor = LibraryRun.objects.create(
            library_id="L2101754",
            instrument_run_id="220121_A00130_0197_BHWJ2HDSX2",
            run_id="r.197",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor_topup = LibraryRun.objects.create(
            library_id="L2101754",  # L2101754_topup
            instrument_run_id="220204_A01052_0076_AH3TLLDSX3",
            run_id="r.76",
            lane=2,
            override_cycles="Y151;I8;I8;Y151",
        )

        umccrise_workflow_id = "wfr.e11f701ceabb4c3a8319a44c32847966"  # prod
        wts_workflow_id = "wfr.b4d407d5269d4dd8973bc6bd9e924237"  # prod

        #  ---

        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2021", "2022"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
        mock_wts_workflow: Workflow = DragenWtsWorkflowFactory()

        umccrise_wfl_run = wes.get_run(umccrise_workflow_id, to_dict=True)
        mock_umccrise_workflow.wfr_id = umccrise_workflow_id
        mock_umccrise_workflow.input = json.dumps(umccrise_wfl_run['input'])
        mock_umccrise_workflow.output = json.dumps(umccrise_wfl_run['output'])
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_normal)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor)
        mock_umccrise_workflow.end = now()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        wts_wfl_run = wes.get_run(wts_workflow_id, to_dict=True)
        mock_wts_workflow.wfr_id = wts_workflow_id
        mock_wts_workflow.input = json.dumps(wts_wfl_run['input'])
        mock_wts_workflow.output = json.dumps(wts_wfl_run['output'])
        mock_wts_workflow.libraryrun_set.add(wts_library_tumor)
        mock_wts_workflow.libraryrun_set.add(wts_library_tumor_topup)
        mock_wts_workflow.end = now()
        mock_wts_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow.save()

        job_list = rnasum_step.prepare_rnasum_jobs(this_workflow=mock_umccrise_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)

    @skip
    def test_prepare_rnasum_jobs_SBJ01625(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_prepare_rnasum_jobs_SBJ01625
        """

        # Replaying https://data.umccr.org/subjects/SBJ01625
        # curl -s -X GET -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/libraryrun?library_id=L2200228&instrument_run_id=220224_A01052_0081_BHFCWJDSX3" | jq

        wgs_library_normal = LibraryRun.objects.create(
            library_id="L2200228",
            instrument_run_id="220224_A01052_0081_BHFCWJDSX3",
            run_id="r.81",
            lane=3,
            override_cycles="Y151;I8;I8;Y151",
        )

        wgs_library_tumor = LibraryRun.objects.create(
            library_id="L2200229",
            instrument_run_id="220224_A01052_0081_BHFCWJDSX3",
            run_id="r.81",
            lane=2,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor = LibraryRun.objects.create(
            library_id="L2200194",
            instrument_run_id="220224_A01052_0081_BHFCWJDSX3",
            run_id="r.81",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        umccrise_workflow_id = "wfr.5ceda40be069408b9c7e112afe9b1f71"  # prod
        wts_workflow_id = "wfr.385f082d5dea43ecac38e05ce3769109"  # prod

        #  ---

        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2021", "2022"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
        mock_wts_workflow: Workflow = DragenWtsWorkflowFactory()

        umccrise_wfl_run = wes.get_run(umccrise_workflow_id, to_dict=True)
        mock_umccrise_workflow.wfr_id = umccrise_workflow_id
        mock_umccrise_workflow.input = json.dumps(umccrise_wfl_run['input'])
        mock_umccrise_workflow.output = json.dumps(umccrise_wfl_run['output'])
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_normal)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor)
        mock_umccrise_workflow.end = now()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        wts_wfl_run = wes.get_run(wts_workflow_id, to_dict=True)
        mock_wts_workflow.wfr_id = wts_workflow_id
        mock_wts_workflow.input = json.dumps(wts_wfl_run['input'])
        mock_wts_workflow.output = json.dumps(wts_wfl_run['output'])
        mock_wts_workflow.libraryrun_set.add(wts_library_tumor)
        mock_wts_workflow.end = now()
        mock_wts_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow.save()

        job_list = rnasum_step.prepare_rnasum_jobs(this_workflow=mock_umccrise_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)

    @skip
    def test_prepare_rnasum_jobs_SBJ00910(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_prepare_rnasum_jobs_SBJ00910
        """

        # Replaying https://data.dev.umccr.org/subjects/SBJ00910

        wgs_library_normal = LibraryRun.objects.create(
            library_id="L2100745",
            instrument_run_id="210708_A00130_0166_AH7KTJDSX2",
            run_id="r.166",
            lane=1,
            override_cycles="Y151;I8N2;I8N2;Y151",
        )

        wgs_library_tumor = LibraryRun.objects.create(
            library_id="L2100746",
            instrument_run_id="210708_A00130_0166_AH7KTJDSX2",
            run_id="r.166",
            lane=1,
            override_cycles="Y151;I8N2;I8N2;Y151",
        )

        wts_library_tumor = LibraryRun.objects.create(
            library_id="L2100732",
            instrument_run_id="210708_A00130_0166_AH7KTJDSX2",
            run_id="r.166",
            lane=1,
            override_cycles="Y151;I8N2;I8N2;Y151",
        )

        umccrise_workflow_id = "wfr.cf4b46bc5ea346678200cd8ad2bf3b65"  # in DEV
        wts_workflow_id = "wfr.b61f86b3ac2748fe997ecdf1d4b79d84"       # in DEV

        #  ---

        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2021", "2022"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
        mock_wts_workflow: Workflow = DragenWtsWorkflowFactory()

        umccrise_wfl_run = wes.get_run(umccrise_workflow_id, to_dict=True)
        mock_umccrise_workflow.wfr_id = umccrise_workflow_id
        mock_umccrise_workflow.input = json.dumps(umccrise_wfl_run['input'])
        mock_umccrise_workflow.output = json.dumps(umccrise_wfl_run['output'])
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_normal)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor)
        mock_umccrise_workflow.end = now()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        wts_wfl_run = wes.get_run(wts_workflow_id, to_dict=True)
        mock_wts_workflow.wfr_id = wts_workflow_id
        mock_wts_workflow.input = json.dumps(wts_wfl_run['input'])
        mock_wts_workflow.output = json.dumps(wts_wfl_run['output'])
        mock_wts_workflow.libraryrun_set.add(wts_library_tumor)
        mock_wts_workflow.end = now()
        mock_wts_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow.save()

        job_list = rnasum_step.prepare_rnasum_jobs(this_workflow=mock_umccrise_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)

    @skip
    def test_prepare_rnasum_jobs_SBJ02060(self):
        """
        unset ICA_ACCESS_TOKEN
        export AWS_PROFILE=prod
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_prepare_rnasum_jobs_SBJ02060

        SBJ02060 is a good example for recurring sequencing sample. i.e. 2 tumor each for WGS, WTS and TSO
        See https://github.com/umccr/data-portal-apis/issues/547
        """

        # Replaying https://portal.umccr.org/subjects/SBJ02060

        # Populate test db with 2 SequenceRun
        sqr_93: SequenceRun = SequenceRunFactory()
        sqr_93.run_id = "r.93"
        sqr_93.name = "220414_A01052_0093_BH7L55DMXY"
        sqr_93.instrument_run_id = "220414_A01052_0093_BH7L55DMXY"
        sqr_93.status = "PendingAnalysis"
        sqr_93.date_modified = make_aware(datetime.strptime("2022-04-16 00:38:21", "%Y-%m-%d %H:%M:%S"))
        sqr_93.save()

        sqr_233: SequenceRun = SequenceRunFactory2()
        sqr_233.run_id = "r.233"
        sqr_233.name = "221019_A00130_0233_BH53K5DSX5"
        sqr_233.instrument_run_id = "221019_A00130_0233_BH53K5DSX5"
        sqr_233.status = "PendingAnalysis"
        sqr_233.date_modified = make_aware(datetime.strptime("2022-10-20 18:11:00", "%Y-%m-%d %H:%M:%S"))
        sqr_233.save()

        # Populate test db with LibraryRun records for WGS and WTS libraries
        wgs_library_normal = LibraryRun.objects.create(
            library_id="L2200460",
            instrument_run_id="220414_A01052_0093_BH7L55DMXY",
            run_id="r.93",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        wgs_library_tumor = LibraryRun.objects.create(
            library_id="L2201539",
            instrument_run_id="221019_A00130_0233_BH53K5DSX5",
            run_id="r.233",
            lane=3,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor_1 = LibraryRun.objects.create(
            library_id="L2200449",
            instrument_run_id="220414_A01052_0093_BH7L55DMXY",
            run_id="r.93",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor_2 = LibraryRun.objects.create(
            library_id="L2201545",
            instrument_run_id="221019_A00130_0233_BH53K5DSX5",
            run_id="r.233",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        # Populate test db with LabMetadata from Sheet 2022
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2022"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # Populate test db with the actual WorkflowRun for WGS and WTS libraries from PROD
        # We will use wes.get_run(...) to sync from ICA to test db
        umccrise_workflow_id = "wfr.e3b09354992f4e99b3c8a99f8a68f74e"  # in PROD for L2201539
        wts_workflow_id_1 = "wfr.a11d74a3daf447aa82c66c902f4d10de"  # in PROD for L2200449
        wts_workflow_id_2 = "wfr.d4367786b1b0469ca090bda4a887bb57"  # in PROD for L2201545

        mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
        mock_wts_workflow_1: Workflow = DragenWtsWorkflowFactory()
        mock_wts_workflow_2: Workflow = DragenWtsWorkflowFactory2()

        umccrise_wfl_run = wes.get_run(umccrise_workflow_id, to_dict=True)
        mock_umccrise_workflow.wfr_id = umccrise_workflow_id
        mock_umccrise_workflow.input = json.dumps(umccrise_wfl_run['input'])
        mock_umccrise_workflow.output = json.dumps(umccrise_wfl_run['output'])
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_normal)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor)
        mock_umccrise_workflow.end = now()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        wts_wfl_run_1 = wes.get_run(wts_workflow_id_1, to_dict=True)
        mock_wts_workflow_1.wfr_id = wts_workflow_id_1
        mock_wts_workflow_1.input = json.dumps(wts_wfl_run_1['input'])
        mock_wts_workflow_1.output = json.dumps(wts_wfl_run_1['output'])
        mock_wts_workflow_1.libraryrun_set.add(wts_library_tumor_1)
        mock_wts_workflow_1.end = now()
        mock_wts_workflow_1.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow_1.save()

        wts_wfl_run_2 = wes.get_run(wts_workflow_id_2, to_dict=True)
        mock_wts_workflow_2.wfr_id = wts_workflow_id_2
        mock_wts_workflow_2.input = json.dumps(wts_wfl_run_2['input'])
        mock_wts_workflow_2.output = json.dumps(wts_wfl_run_2['output'])
        mock_wts_workflow_2.libraryrun_set.add(wts_library_tumor_2)
        mock_wts_workflow_2.end = now()
        mock_wts_workflow_2.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow_2.save()

        # At this point, we have replayed Pipeline the state such that
        # We have 2 tumor libraries candidate for WTS.
        # We have done umccrise run for the Subject.
        # And the correspondent transcriptome workflow runs have done as well.
        # Now. At this point in orchestration;
        # if we prepare runnable RNAsum job with latest-greatest strategy,
        # this should go with L2201545

        job_list = rnasum_step.prepare_rnasum_jobs(this_workflow=mock_umccrise_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)
        self.assertEqual(job_list[0]['tumor_library_id'], "L2201545")

    @skip
    def test_prepare_rnasum_jobs_SBJ02569(self):
        """
        unset ICA_ACCESS_TOKEN
        export AWS_PROFILE=prod
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_prepare_rnasum_jobs_SBJ02569

        SBJ02569 has 2 WTS libraries -- L2201035 is sequenced, L2200955 is not.
        See https://github.com/umccr/data-portal-apis/issues/548
        """

        # Replaying https://portal.umccr.org/subjects/SBJ02569

        # Populate test db with SequenceRun
        sqr_111: SequenceRun = SequenceRunFactory()
        sqr_111.run_id = "r.111"
        sqr_111.name = "220729_A01052_0111_AHVJCYDSX3"
        sqr_111.instrument_run_id = "220729_A01052_0111_AHVJCYDSX3"
        sqr_111.status = "PendingAnalysis"
        sqr_111.date_modified = make_aware(datetime.strptime("2022-07-30 19:22:04", "%Y-%m-%d %H:%M:%S"))
        sqr_111.save()

        # Populate test db with LibraryRun records for WGS and WTS libraries
        wgs_library_normal = LibraryRun.objects.create(
            library_id="L2200967",
            instrument_run_id="220722_A01052_0110_AHVGVFDSX3",
            run_id="r.110",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        wgs_library_tumor = LibraryRun.objects.create(
            library_id="L2200985",
            instrument_run_id="220722_A01052_0109_BHVHJJDSX3",
            run_id="r.109",
            lane=3,
            override_cycles="Y151;I8;I8;Y151",
        )

        wts_library_tumor = LibraryRun.objects.create(
            library_id="L2201035",
            instrument_run_id="220729_A01052_0111_AHVJCYDSX3",
            run_id="r.111",
            lane=1,
            override_cycles="Y151;I8;I8;Y151",
        )

        # Populate test db with LabMetadata from Sheet 2022
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2022"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # Populate test db with the actual WorkflowRun for WGS and WTS libraries from PROD
        # We will use wes.get_run(...) to sync from ICA to test db
        umccrise_workflow_id = "wfr.194a9c82d0904426950a36e70be34ad9"  # in PROD for L2200985
        wts_workflow_id = "wfr.60c79a70f5f3419eb7ba95bdfa194681"  # in PROD for L2201035

        mock_umccrise_workflow: Workflow = UmccriseWorkflowFactory()
        mock_wts_workflow: Workflow = DragenWtsWorkflowFactory()

        umccrise_wfl_run = wes.get_run(umccrise_workflow_id, to_dict=True)
        mock_umccrise_workflow.wfr_id = umccrise_workflow_id
        mock_umccrise_workflow.input = json.dumps(umccrise_wfl_run['input'])
        mock_umccrise_workflow.output = json.dumps(umccrise_wfl_run['output'])
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_normal)
        mock_umccrise_workflow.libraryrun_set.add(wgs_library_tumor)
        mock_umccrise_workflow.end = now()
        mock_umccrise_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_umccrise_workflow.save()

        wts_wfl_run_1 = wes.get_run(wts_workflow_id, to_dict=True)
        mock_wts_workflow.wfr_id = wts_workflow_id
        mock_wts_workflow.input = json.dumps(wts_wfl_run_1['input'])
        mock_wts_workflow.output = json.dumps(wts_wfl_run_1['output'])
        mock_wts_workflow.libraryrun_set.add(wts_library_tumor)
        mock_wts_workflow.end = now()
        mock_wts_workflow.end_status = WorkflowStatus.SUCCEEDED.value
        mock_wts_workflow.save()

        # At this point, we have replayed Pipeline the state such that
        # We have 2022 LabMetadata state; that include 2 WTS libraries for ths Subject. One sequenced, another is not.
        # We have 1 WTS tumor library completed for transcriptome workflow.
        # We have done umccrise run for the Subject.
        # Now. At this point in orchestration;
        # if we prepare runnable RNAsum job,
        # this should _still_ go with L2201035

        job_list = rnasum_step.prepare_rnasum_jobs(this_workflow=mock_umccrise_workflow)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(f"\n{json.dumps(job_list)}")
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), 1)
        self.assertEqual(job_list[0]['tumor_library_id'], "L2201035")

    @skip
    def test_lookup_tcga_dataset(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_rnasum_step.RNAsumStepIntegrationTests.test_lookup_tcga_dataset
        """
        pass
