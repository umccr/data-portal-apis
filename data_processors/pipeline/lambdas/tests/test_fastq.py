from unittest import skip

from libica.openapi import libgds
from mockito import when

from data_processors.lims.lambdas import labmetadata
from data_processors.pipeline.lambdas import fastq
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class FastQUnitTests(PipelineUnitTestCase):

    def test_parse_gds_path(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq.FastQUnitTests.test_parse_gds_path
        """
        gds_path = "gds://raw-sequence-data-dev/999999_Z99999_0010_AG2CTTAGCT/SampleSheet.csv"
        path_elements = gds_path.replace("gds://", "").split("/")
        logger.info(path_elements)

        volume_name = path_elements[0]
        path = path_elements[1:]
        logger.info(volume_name)
        logger.info(path)
        logger.info(f"/{'/'.join(path)}/*")

        run_id = "300101_A99999_0020_AG2CTTAGYY"
        new_gds_path = f"gds://{path_elements[0]}/{run_id}/{path_elements[2]}"
        logger.info(new_gds_path)

        v, p = fastq.parse_gds_path(gds_path)
        self.assertEqual(v, "raw-sequence-data-dev")
        self.assertEqual(p, "/999999_Z99999_0010_AG2CTTAGCT/SampleSheet.csv")

    def test_extract_fastq_sample_name(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq.FastQUnitTests.test_extract_fastq_sample_name
        """
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
            sample_name = fastq.extract_fastq_sample_name(name)
            logger.info((sample_name, name))
            self.assertTrue("_R" not in sample_name)

        self.assertIsNone(fastq.extract_fastq_sample_name("L1999999_topup_R1_001.fastq.gz"))

    def test_fastq_map_build(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq.FastQUnitTests.test_fastq_map_build
        """
        wfr_id = f"wfr.{_rand(32)}"
        locations = [f"gds://{wfr_id}/bclConversion_launch/try-1/out-dir-bclConvert", ]

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

        fastq_container: dict = fastq.handler({'locations': locations}, None)

        for sample_name, bag in fastq_container['fastq_map'].items():
            fastq_list = bag['fastq_list']
            logger.info((sample_name, fastq_list))
        self.assertEqual(4, len(fastq_container['fastq_map'].keys()))  # assert sample count is 4

    def test_fastq_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq.FastQUnitTests.test_fastq_handler
        """
        self.verify_local()
        fastq.handler({'locations': ["gds://anything/work/for/hitting/prism/dynamic/mock", ]}, None)
        # monitor mock container if you like:  docker logs -f data-portal-apis_gds_1


class FastQIntegrationTests(PipelineIntegrationTestCase):
    # integration test cases are meant to hit IAP endpoints

    @skip
    def test_fastq_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq.FastQIntegrationTests.test_fastq_handler
        """

        # populate test db with LabMetadata
        stat = labmetadata.scheduled_update_handler({'event': "FastQIntegrationTests", 'truncate': False}, None)
        logger.info(f"{stat}")

        event = {
            'locations': [
                "gds://development/primary_data/200612_A01052_0017_BH5LYWDSXY",
            ],
            # 'project_owner': [
            #     "UMCCR",
            # ],
            # 'flat': True,
        }

        # event = {
        #     'locations': [
        #         "gds://production/primary_data/210923_A00130_0176_BHH5JFDSX2",
        #     ],
        #     'project_owner': [
        #         "Scott",
        #         "Hovens",
        #     ],
        #     # 'flat': True,
        # }

        fastq_container: dict = fastq.handler(event, None)

        self.assertIsNotNone(fastq_container)
        # self.assertEqual(8, len(fastq_container['fastq_map'].keys()))
