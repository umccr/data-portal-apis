import json
from datetime import datetime
from unittest import skip

from django.utils.timezone import make_aware
from libica.app import wes
from libica.openapi import libwes
from libumccr.aws import libssm
from mockito import when, spy2

from data_portal.models.batch import Batch
from data_portal.models.batchrun import BatchRun
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.orchestration import dragen_wgs_qc_step, fastq_update_step
from data_processors.pipeline.services import batch_srv, fastq_srv, libraryrun_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

tn_mock_subject_id = "SBJ00001"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"


class DragenWgsQcStepUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(DragenWgsQcStepUnitTests, self).setUp()

    def test_dragen_wgs_qc(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepUnitTests.test_dragen_wgs_qc
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()
        mock_bcl_workflow.input = json.dumps({
            'bcl_input_directory': {
                "class": "Directory",
                "location": "gds://bssh-path/Runs/210701_A01052_0055_AH7KWGDSX2_r.abc123456"
            }
        })
        mock_bcl_workflow.save()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = {
            "main/fastq_list_rows": [
                {
                    "rgid": "CATGCGAT.4",
                    "rglb": "UnknownLibrary",
                    "rgsm": mock_sample_name,
                    "lane": 4,
                    "read_1": {
                        "class": "File",
                        "basename": f"{mock_sample_name}_S1_L004_R1_001.fastq.gz",
                        "location": f"gds://fastqvol/bcl-convert-test/outputs/10X/{mock_sample_name}_S1_L004_R1_001.fastq.gz",
                        "nameroot": f"{mock_sample_name}_S1_L004_R1_001.fastq",
                        "nameext": ".gz",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 16698849950
                    },
                    "read_2": {
                        "class": "File",
                        "basename": f"{mock_sample_name}_S1_L004_R2_001.fastq.gz",
                        "location": f"gds://fastqvol/bcl-convert-test/outputs/10X/{mock_sample_name}_S1_L004_R2_001.fastq.gz",
                        "nameroot": f"{mock_sample_name}_S1_L004_R2_001.fastq",
                        "nameext": ".gz",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 38716143739
                    }
                }
            ],
            "split_sheets": [
                {
                    "location": "gds://umccr-fastq-data-prod/210701_A01052_0055_AH7KWGDSX2/SampleSheet.ctDNA_ctTSO.csv",
                    "basename": "SampleSheet.ctDNA_ctTSO.csv",
                    "nameroot": "SampleSheet.ctDNA_ctTSO",
                    "nameext": ".csv",
                    "class": "File",
                    "size": 1804,
                    "http://commonwl.org/cwltool#generation": 0
                }
            ]
        }

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        #LPRJ200438
        mock_labmetadata_tumor = LabMetadata()
        mock_labmetadata_tumor.subject_id = tn_mock_subject_id
        mock_labmetadata_tumor.library_id = mock_library_id
        mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
        mock_labmetadata_tumor.type = LabMetadataType.WGS.value
        mock_labmetadata_tumor.save()

        # ignore step_skip_list
        spy2(libssm.get_ssm_param)
        when(libssm).get_ssm_param(f"{ICA_WORKFLOW_PREFIX}/step_skip_list").thenReturn(json.dumps([]))

        result = orchestrator.handler({
            'wfr_id': TestConstant.wfr_id.value,
            'wfv_id': TestConstant.wfv_id.value,
        }, None)

        logger.info("-" * 32)
        self.assertIsNotNone(result)
        logger.info(f"Orchestrator lambda call output: \n{json.dumps(result)}")

        for b in Batch.objects.all():
            logger.info(f"BATCH: {b}")
        for br in BatchRun.objects.all():
            logger.info(f"BATCH_RUN: {br}")

        wgs_qc_batch_runs = [br for br in BatchRun.objects.all() if br.step == WorkflowType.DRAGEN_WGS_QC.value]

        self.assertTrue(wgs_qc_batch_runs[0].running)

    def test_dragen_wgs_qc_none(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepUnitTests.test_dragen_wgs_qc_none

        Similar to ^^ test case but BCL Convert output is None
        """
        self.verify_local()

        mock_bcl_workflow: Workflow = WorkflowFactory()

        mock_wfl_run = libwes.WorkflowRun()
        mock_wfl_run.id = TestConstant.wfr_id.value
        mock_wfl_run.status = WorkflowStatus.SUCCEEDED.value
        mock_wfl_run.time_stopped = make_aware(datetime.utcnow())
        mock_wfl_run.output = None  # mock output is NONE while status is SUCCEEDED

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        try:
            orchestrator.handler({
                'wfr_id': TestConstant.wfr_id.value,
                'wfv_id': TestConstant.wfv_id.value,
            }, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)


class DragenWgsQcStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_dragen_wgs_qc_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepIntegrationTests.test_prepare_dragen_wgs_qc_jobs
        """

        # --- pick one successful BCL Convert run
        # ica workflows runs list
        # ica workflows runs get wfr.<ID>

        bcl_convert_wfr_id = "wfr.18210c790f30452992c5fd723521f014"  # from PROD
        total_jobs_to_eval = 12

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({
            'sheets': ["2020", "2021"],
            'truncate': False
        }, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # second --
        # - we need to have BCL Convert workflow in db
        # - WorkflowFactory also create related fixture sub factory SequenceRunFactory and linked them
        mock_bcl_convert: Workflow = WorkflowFactory()

        # third --
        # - grab workflow run from WES endpoint
        # - sync input and output attributes to our mock BCL Convert workflow in db
        bcl_convert_run = wes.get_run(bcl_convert_wfr_id, to_dict=True)
        mock_bcl_convert.input = json.dumps(bcl_convert_run['input'])
        mock_bcl_convert.output = json.dumps(bcl_convert_run['output'])
        mock_bcl_convert.save()

        # fourth --
        # - replay FastqListRow update step after BCL Convert workflow succeeded
        fastq_update_step.perform(mock_bcl_convert)

        # fifth --
        # - we also need Batch and BatchRun since DRAGEN_WGS_QC workflows (jobs) are running in batch manner
        # - we will use Batcher to create them, just like in dragen_wgs_qc_step.perform()
        batcher = Batcher(
            workflow=mock_bcl_convert,
            run_step=WorkflowType.DRAGEN_WGS_QC.value,
            batch_srv=batch_srv,
            fastq_srv=fastq_srv,
            logger=logger
        )

        logger.info("-" * 32)
        logger.info("PREPARE DRAGEN_WGS_QC JOBS:")

        job_list = dragen_wgs_qc_step.prepare_dragen_wgs_qc_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)

    @skip
    def test_prepare_dragen_wgs_qc_jobs_197(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_wgs_qc_step.DragenWgsQcStepIntegrationTests.test_prepare_dragen_wgs_qc_jobs_197
        """

        # --- pick one successful BCL Convert run in development project

        # 220121_A00130_0197_BHWJ2HDSX2 in PROD

        # https://umccr.slack.com/archives/C8CG6K76W/p1642743131000500
        bssh_run_event = {
            'gds_volume_name': "bssh.acddbfda498038ed99fa94fe79523959",
            'gds_folder_path': "/Runs/220121_A00130_0197_BHWJ2HDSX2_r.7wPth2L-GkmHlLRSvjfiZw",
            'instrument_run_id': "220121_A00130_0197_BHWJ2HDSX2",
            'run_id': "r.7wPth2L-GkmHlLRSvjfiZw",
        }

        # https://umccr.slack.com/archives/C8CG6K76W/p1644389695027599
        bcl_convert_wfr_id = "wfr.e7cd80eee78e425ca94507f505315e9b"  # this is re-run conversion
        total_jobs_to_eval = 17  # all library should re-run QC steps as this is induced by re-run new bcl conversion

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        def deep_replay():
            from data_processors.pipeline.lambdas import libraryrun
            libraryrun.handler(bssh_run_event, None)

            # replay initial bcl conversion

            # https://umccr.slack.com/archives/C8CG6K76W/p1643027423017400
            init_bcl_convert_wfr_id = "wfr.49ceee1c20b64ca9a5abd2beb663ee57"
            init_bcl_convert_run = wes.get_run(init_bcl_convert_wfr_id, to_dict=True)

            mock_init_bcl_convert: Workflow = WorkflowFactory()
            mock_init_bcl_convert.wfr_id = init_bcl_convert_wfr_id
            mock_init_bcl_convert.input = json.dumps(init_bcl_convert_run['input'])
            mock_init_bcl_convert.output = json.dumps(init_bcl_convert_run['output'])
            mock_init_bcl_convert.sequence_run = sqr
            mock_init_bcl_convert.save()

            # replay initial BatchRun state
            init_batcher = Batcher(
                workflow=mock_init_bcl_convert,
                run_step=WorkflowType.DRAGEN_WGS_QC.value,
                batch_srv=batch_srv,
                fastq_srv=fastq_srv,
                logger=logger
            )
            init_batch_run: BatchRun = init_batcher.batch_run
            init_batch_run.running = False
            init_batch_run.notified = True
            init_batch_run.save()

            # https://umccr.slack.com/archives/C8CG6K76W/p1643037681017700
            run_pairs = [
                ("L2101734", "wfr.dab14dc2b0094e1aba36f0639aeee4d7"),
                ("L2101385", "wfr.9794734828cd4395a84446a05109682f"),
                ("L2101735", "wfr.db55f6a594154a88aeb959eca90d4c38"),
                ("L2101386", "wfr.074009be91a54151af86eb3156358474"),
                ("L2101651", "wfr.fd80ba531de64cdcb54499266e917189"),
                ("L2101736", "wfr.e6346ebf28dd4613b0921c7b8e09b7e8"),
                ("L2101654", "wfr.8a7618da65414b06b52c0e13632f29e0"),
                ("L2101737", "wfr.4ea785ea13d84e49ab33147de78cc193"),
                ("L2101658", "wfr.985f8094d6c9421fafd0b28e1bed5641"),
                ("L2200071", "wfr.0307389e701d48c69712b8570ca859b1"),
                ("L2101729", "wfr.64645e8a672f4624a1ce8d15ffcb233c"),
                ("L2200072", "wfr.50a77e412ae04ab99de2a97ac7cbbb57"),
                ("L2101730", "wfr.6a3bdd6836404076991df6a45080247b"),
                ("L2200072", "wfr.3082d29340364d039517f274078757f1"),
                ("L2101731", "wfr.bc6f98938f44400fa418e4c9ca82e33d"),
                ("L2101732", "wfr.d96c950dfce346209b8e071facd43ce3"),
                ("L2101733", "wfr.c70a2697c0f5465ea1f3abbb681acfa0"),
            ]

            # nonlocal total_jobs_to_eval
            # total_jobs_to_eval = 17  # overwrite evaluation

            for run_pair in run_pairs:
                library_id, wfr_id = run_pair
                wfr = wes.get_run(wfr_id=wfr_id)
                wfl = Workflow.objects.create(
                    wfr_name=wfr.name,
                    type_name=WorkflowType.DRAGEN_TSO_CTDNA.value,
                    wfr_id=wfr.id,
                    wfv_id=wfr.workflow_version.id,
                    version=wfr.workflow_version.version,
                    input=wfr.input,
                    start=wfr.time_started if wfr.time_started else wfr.time_created,
                    output=wfr.output,
                    end=wfr.time_stopped if wfr.time_stopped else wfr.time_modified,
                    end_status=wfr.status,
                    sequence_run=sqr,
                    batch_run=init_batch_run,
                )
                libraryrun_srv.link_library_runs_with_workflow(library_id=library_id, workflow=wfl)

        # first --
        # - we need metadata!
        # - populate LabMetadata tables in test db
        from data_processors.lims.lambdas import labmetadata
        labmetadata.scheduled_update_handler({'event': "test_prepare_dragen_tso_ctdna_jobs_197"}, None)
        logger.info(f"Lab metadata count: {LabMetadata.objects.count()}")

        # second --
        # - we need to have BCL Convert workflow in db
        # - WorkflowFactory also create related fixture sub factory SequenceRunFactory and linked them
        mock_bcl_convert: Workflow = WorkflowFactory()
        sqr = mock_bcl_convert.sequence_run
        sqr.run_id = bssh_run_event['run_id']
        sqr.instrument_run_id = bssh_run_event['instrument_run_id']
        sqr.name = sqr.instrument_run_id
        sqr.save()

        # third --
        # - grab workflow run from WES endpoint
        # - sync input and output attributes to our mock BCL Convert workflow in db
        bcl_convert_run = wes.get_run(bcl_convert_wfr_id, to_dict=True)
        mock_bcl_convert.wfr_id = bcl_convert_wfr_id
        mock_bcl_convert.input = json.dumps(bcl_convert_run['input'])
        mock_bcl_convert.output = json.dumps(bcl_convert_run['output'])
        mock_bcl_convert.save()

        # Optionally we could perform deep replay
        deep_replay()

        # fourth --
        # - replay FastqListRow update step after BCL Convert workflow succeeded
        fastq_update_step.perform(mock_bcl_convert)

        # fifth --
        # - we also need Batch and BatchRun since workflows (jobs) are running in batch manner
        # - we will use Batcher to create them, just like in dragen_wgs_qc_step.perform()
        batcher = Batcher(
            workflow=mock_bcl_convert,
            run_step=WorkflowType.DRAGEN_TSO_CTDNA.value,
            batch_srv=batch_srv,
            fastq_srv=fastq_srv,
            logger=logger
        )

        logger.info("-" * 32)
        logger.info("PREPARE DRAGEN_TSO_CTDNA JOBS:")

        job_list = dragen_wgs_qc_step.prepare_dragen_wgs_qc_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)

        # assert that we have 2 distinct Batch
        self.assertEqual(Batch.objects.count(), 2)
        logger.info("-" * 32)
        for br in Batch.objects.all():
            logger.info(f"{br}")
