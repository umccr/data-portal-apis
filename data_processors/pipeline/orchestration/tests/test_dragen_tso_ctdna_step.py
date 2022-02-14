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
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType, LabMetadataAssay, \
    LabMetadataWorkflow
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import WorkflowFactory, TestConstant
from data_processors.pipeline.domain.batch import Batcher
from data_processors.pipeline.domain.config import ICA_WORKFLOW_PREFIX
from data_processors.pipeline.domain.workflow import WorkflowStatus, WorkflowType
from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.orchestration import fastq_update_step, dragen_tso_ctdna_step
from data_processors.pipeline.services import batch_srv, fastq_srv, libraryrun_srv
from data_processors.pipeline.tests.case import PipelineIntegrationTestCase, PipelineUnitTestCase, logger

tn_mock_subject_id = "SBJ00001"
mock_library_id = "LPRJ200438"
mock_sample_id = "PRJ200438"
mock_sample_name = f"{mock_sample_id}_{mock_library_id}"


def _mock_bcl_convert_output():
    return {
        "fastq_list_rows": [
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


class DragenTsoCtDnaStepUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(DragenTsoCtDnaStepUnitTests, self).setUp()

    def test_dragen_tso_ctdna(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_dragen_tso_ctdna
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
        mock_wfl_run.output = _mock_bcl_convert_output()

        workflow_version: libwes.WorkflowVersion = libwes.WorkflowVersion()
        workflow_version.id = TestConstant.wfv_id.value
        mock_wfl_run.workflow_version = workflow_version
        when(libwes.WorkflowRunsApi).get_workflow_run(...).thenReturn(mock_wfl_run)

        # LPRJ200438
        mock_labmetadata_tumor = LabMetadata()
        mock_labmetadata_tumor.subject_id = tn_mock_subject_id
        mock_labmetadata_tumor.library_id = mock_library_id
        mock_labmetadata_tumor.phenotype = LabMetadataPhenotype.TUMOR.value
        mock_labmetadata_tumor.type = LabMetadataType.CT_DNA.value
        mock_labmetadata_tumor.assay = LabMetadataAssay.CT_TSO.value
        mock_labmetadata_tumor.workflow = LabMetadataWorkflow.RESEARCH.value
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

        tso_ctdna_batch_runs = [br for br in BatchRun.objects.all() if br.step == WorkflowType.DRAGEN_TSO_CTDNA.value]
        self.assertTrue(tso_ctdna_batch_runs[0].running)

    def test_dragen_tso_ctdna_none(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_dragen_tso_ctdna_none

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

    def test_get_ct_tso_samplesheet_from_bcl_convert_output(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_get_ct_tso_samplesheet_from_bcl_convert_output
        """
        mock_bcl_convert_output_json_str = json.dumps(_mock_bcl_convert_output())
        ss = dragen_tso_ctdna_step.get_ct_tso_samplesheet_from_bcl_convert_output(mock_bcl_convert_output_json_str)
        logger.info(ss)
        self.assertTrue(ss.startswith('gds://'))
        self.assertTrue(ss.endswith('.csv'))

    def test_get_run_xml_files_from_bcl_convert_workflow(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepUnitTests.test_get_run_xml_files_from_bcl_convert_workflow
        """
        mock_bcl_convert_input = json.dumps({
            'bcl_input_directory': {
                "class": "Directory",
                "location": "gds://bssh-path/Runs/210701_A01052_0055_AH7KWGDSX2_r.abc123456"
            }
        })

        xml_files = dragen_tso_ctdna_step.get_run_xml_files_from_bcl_convert_workflow(mock_bcl_convert_input)
        for file in xml_files:
            logger.info(file)
            self.assertTrue(file.startswith('gds://'))
            self.assertTrue(file.endswith('.xml'))


class DragenTsoCtDnaStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_prepare_dragen_tso_ctdna_jobs(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepIntegrationTests.test_prepare_dragen_tso_ctdna_jobs
        """

        # --- pick one successful BCL Convert run

        bcl_convert_wfr_id = "wfr.097dc05051b44c0c8717b32d89dfcf81"  # 210429_A00130_0157_BH3N3FDSX2 in PROD
        total_jobs_to_eval = 15

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
        mock_bcl_convert.wfr_id = bcl_convert_wfr_id
        mock_bcl_convert.input = json.dumps(bcl_convert_run['input'])
        mock_bcl_convert.output = json.dumps(bcl_convert_run['output'])
        mock_bcl_convert.save()

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

        job_list = dragen_tso_ctdna_step.prepare_dragen_tso_ctdna_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)

    @skip
    def test_prepare_dragen_tso_ctdna_jobs_70(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepIntegrationTests.test_prepare_dragen_tso_ctdna_jobs_70
        """

        # --- pick one successful BCL Convert run in development project

        # 211213_A01052_0070_BH35KVDMXY in PROD

        # https://umccr.slack.com/archives/C8CG6K76W/p1639502718024500
        bssh_run_event = {
            'gds_volume_name': "bssh.acddbfda498038ed99fa94fe79523959",
            'gds_folder_path': "/Runs/211213_A01052_0070_BH35KVDMXY_r.0WSDrjeaoUGp61OMbpwbGQ",
            'instrument_run_id': "211213_A01052_0070_BH35KVDMXY",
            'run_id': "r.0WSDrjeaoUGp61OMbpwbGQ",
        }

        # https://umccr.slack.com/archives/C8CG6K76W/p1639509879024800
        bcl_convert_wfr_id = "wfr.aae6970808bf4b328873b9278777332b"
        total_jobs_to_eval = 9

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        def deep_replay():
            from data_processors.pipeline.lambdas import libraryrun
            libraryrun.handler(bssh_run_event, None)

            # replay initial BatchRun state
            init_batcher = Batcher(
                workflow=mock_bcl_convert,
                run_step=WorkflowType.DRAGEN_TSO_CTDNA.value,
                batch_srv=batch_srv,
                fastq_srv=fastq_srv,
                logger=logger
            )
            init_batch_run: BatchRun = init_batcher.batch_run
            init_batch_run.running = False
            init_batch_run.notified = True
            init_batch_run.save()

            # https://umccr.slack.com/archives/C8CG6K76W/p1639550197031800
            run_pairs = [
                ("L2101514", "wfr.5f812da668334a84b33e926fd02ce3d9"),
                ("L2101515", "wfr.d20c4352ff9f4f099876a95b17cb7824"),
                ("L2101516", "wfr.03e54a22601b44edb7e6c0c51c4b6d4b"),
                ("L2101517", "wfr.afaf17f489994948bdbbf7203a5a1e9a"),
                ("L2101518", "wfr.abbc2a2c8d524a8382e15a1dec0a5e61"),
                ("L2101519", "wfr.ced7c8f3a8ac46d993d3d1f60509830a"),
                ("L2101621", "wfr.42b659ae81374ee48c39e2debd769123"),
                ("L2101622", "wfr.081a3f204d074a599517d5326c795f2c"),
                ("L2101644", "wfr.93f2247d9f8d400db34477f72aa272b5"),
            ]

            nonlocal total_jobs_to_eval
            total_jobs_to_eval = 1  # overwrite evaluation

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
        labmetadata.scheduled_update_handler({
            'sheets': ["2020", "2021"],
            'truncate': False
        }, None)
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

        job_list = dragen_tso_ctdna_step.prepare_dragen_tso_ctdna_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)

    @skip
    def test_prepare_dragen_tso_ctdna_jobs_68(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_dragen_tso_ctdna_step.DragenTsoCtDnaStepIntegrationTests.test_prepare_dragen_tso_ctdna_jobs_68
        """

        # --- pick one successful BCL Convert run in development project

        # 211210_A01052_0068_BH372HDMXY in PROD

        # https://umccr.slack.com/archives/C8CG6K76W/p1639223305000700
        bssh_run_event = {
            'gds_volume_name': "bssh.acddbfda498038ed99fa94fe79523959",
            'gds_folder_path': "/Runs/211210_A01052_0068_BH372HDMXY_r.rDEbzQ1dd0uB60d4Ix5dVQ",
            'instrument_run_id': "211210_A01052_0068_BH372HDMXY",
            'run_id': "r.rDEbzQ1dd0uB60d4Ix5dVQ",
        }

        # https://umccr.slack.com/archives/C8CG6K76W/p1639237411001200
        bcl_convert_wfr_id = "wfr.eb1c64d9fdcf43488bb3f8e35ae26009"
        total_jobs_to_eval = 9

        # --- we need to rewind & replay pipeline state in the test db (like cassette tape, ya know!)

        def deep_replay():
            from data_processors.pipeline.lambdas import libraryrun
            libraryrun.handler(bssh_run_event, None)

            # replay initial BatchRun state
            init_batcher = Batcher(
                workflow=mock_bcl_convert,
                run_step=WorkflowType.DRAGEN_TSO_CTDNA.value,
                batch_srv=batch_srv,
                fastq_srv=fastq_srv,
                logger=logger
            )
            init_batch_run: BatchRun = init_batcher.batch_run
            init_batch_run.running = False
            init_batch_run.notified = True
            init_batch_run.save()

            run_pairs = [
                # https://umccr.slack.com/archives/C8CG6K76W/p1639293539003900
                ("L2101498", "wfr.06f8301aacfe414b853d1a2dd1fc51ac"),
                ("L2101499", "wfr.4ef8316d6598496da9e3736c3303e314"),
                ("L2101500", "wfr.78517fff6cd94be3973a47bb4b03d07d"),
                ("L2101501", "wfr.00cb43e2a93c4680a2d6ec312748f5c1"),
                ("L2101502", "wfr.90b30d0b1ac34532b5cd98c57f594c6a"),
                ("L2101503", "wfr.5369e306614644888e1960eff8e96454"),
                ("L2101505", "wfr.7c2c0c4323794689aae775153be816ed"),
                ("L2101506", "wfr.9452b3a85ea84e56ba6996df4d7123de"),
                ("L2101520", "wfr.6f731313eace449891c3f8ba85d5d50f"),

                # https://umccr.slack.com/archives/C8CG6K76W/p1639500685024300
                ("L2101498", "wfr.24dae4f1dc814c50a73536251858758b"),
                ("L2101499", "wfr.2c260b9a03c04342896dccd4fe965d19"),
                ("L2101500", "wfr.bbd175bd92b74ee58eab8eb8194360c6"),
                ("L2101501", "wfr.71474e76269c427ab3166556df98cd33"),
                ("L2101502", "wfr.80aae1ea7fdd45d79165dcdde7157452"),
                ("L2101503", "wfr.46afd96e0c7249bbae0734af7a8eece5"),
                ("L2101505", "wfr.f1fbe321c4c341428a56e93a0deb6196"),
                ("L2101506", "wfr.f05d34ff83e449c489e86d45c6e8c17b"),
                ("L2101520", "wfr.e4e5c37b12154a499eb0a2a32d683260"),
            ]

            nonlocal total_jobs_to_eval
            total_jobs_to_eval = 5  # overwrite evaluation

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
        labmetadata.scheduled_update_handler({
            'sheets': ["2020", "2021"],
            'truncate': False
        }, None)
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

        job_list = dragen_tso_ctdna_step.prepare_dragen_tso_ctdna_jobs(batcher)

        logger.info("-" * 32)
        logger.info("JOB LIST JSON:")
        logger.info(json.dumps(job_list))
        logger.info("YOU SHOULD COPY ABOVE JSON INTO A FILE, FORMAT IT AND CHECK THAT IT LOOKS ALRIGHT")
        self.assertIsNotNone(job_list)
        self.assertEqual(len(job_list), total_jobs_to_eval)
