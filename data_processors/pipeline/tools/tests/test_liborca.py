import json
from unittest import skip

from libica.app import wes

from data_portal.tests.factories import TestConstant
from data_processors.pipeline.tools import liborca
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class LibOrcaUnitTests(PipelineUnitTestCase):

    def test_parse_workflow_output(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_workflow_output
        """
        output: dict = liborca.parse_workflow_output(json.dumps({
            'my_key': {'nested': "Object"}
        }), ['my_key'])

        logger.info("-" * 32)
        logger.info(f"parse_workflow_output: {json.dumps(output)}")

        self.assertIn('nested', output.keys())

    def test_parse_workflow_output_error(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_workflow_output_error
        """
        try:
            _ = liborca.parse_workflow_output("does not matter", [])
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(KeyError)

    def test_parse_umccrise_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_umccrise_workflow_output_directory
        """
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
        result: dict = liborca.parse_umccrise_workflow_output_directory(mock_umccrise_output)

        logger.info("-" * 32)
        logger.info(f"parse_umccrise_workflow_output_directory: {json.dumps(result)}")

        self.assertEqual(result['basename'], "L3200003__L3200004")
        self.assertEqual(result['class'], "Directory")

    def test_parse_transcriptome_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_transcriptome_workflow_output_directory
        """
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
                "location": "gds://vol/analysis_data/SBJ00001/wts_tumor_only/202201232df89fee/PTC_NebRNA12345_dragen_transcriptome_multiqc",
                "basename": "PTC_NebRNA12345_dragen_transcriptome_multiqc",
                "nameroot": "",
                "nameext": "",
                "class": "Directory",
                "size": None
            },
            "output_dir_gds_session_id": "ssn.99999999b45b4f96bc9baf056a79ede2",
            "output_dir_gds_folder_id": "fol.cccccccca064e0362d008d89d4636ab"
        })

        result: dict = liborca.parse_transcriptome_workflow_output_directory(mock_wts_tumor_only_output)

        logger.info("-" * 32)
        logger.info(f"parse_transcriptome_workflow_output_directory: {json.dumps(result)}")

        self.assertEqual(result['basename'], "L3200000_dragen")
        self.assertEqual(result['class'], "Directory")

    def test_parse_somatic_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_somatic_workflow_output_directory
        """
        mock_tn_output = json.dumps({
            "dragen_somatic_output_directory": {
                "basename": "L0000001_L0000002_dragen",
                "class": "Directory",
                "location": "gds://vol/analysis_data/SBJ00001/wgs_tumor_normal/20211208aa4f9099/L0000001_L0000002_dragen",
                "nameext": "",
                "nameroot": "",
                "size": None
            },
        })
        result: dict = liborca.parse_somatic_workflow_output_directory(mock_tn_output)

        logger.info("-" * 32)
        logger.info(f"parse_somatic_workflow_output_directory: {json.dumps(result)}")

        self.assertEqual(result['basename'], "L0000001_L0000002_dragen")
        self.assertEqual(result['class'], "Directory")

    def test_parse_somatic_workflow_output_directory_none(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_somatic_workflow_output_directory_none
        """

        try:
            _ = liborca.parse_somatic_workflow_output_directory(json.dumps({
                "dragen_somatic_output_directory": None
            }))
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_parse_bcl_convert_output(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_bcl_convert_output
        """

        result = liborca.parse_bcl_convert_output(json.dumps({
            "main/fastq_list_rows": [{'rgid': "main/fastq_list_rows"}],
            "fastq_list_rows": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS"}]
        }))

        logger.info("-" * 32)
        logger.info(f"parse_bcl_convert_output: {json.dumps(result)}")

        self.assertEqual(result[0]['rgid'], "main/fastq_list_rows")

    def test_parse_bcl_convert_output_fqlr_none(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_bcl_convert_output_fqlr_none
        """

        try:
            _ = liborca.parse_bcl_convert_output(json.dumps({
                "fastq_list_rows": None
            }))
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(ValueError)

    def test_parse_bcl_convert_output_alt(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_bcl_convert_output_alt
        """

        result = liborca.parse_bcl_convert_output(json.dumps({
            "fastq_list_rows2": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS"}],
            "fastq_list_rows": [{'rgid': "fastq_list_rows"}]
        }))

        logger.info("-" * 32)
        logger.info(f"parse_bcl_convert_output alt: {json.dumps(result)}")

        self.assertEqual(result[0]['rgid'], "fastq_list_rows")

    def test_parse_bcl_convert_output_error(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_bcl_convert_output_error
        """

        try:
            liborca.parse_bcl_convert_output(json.dumps({
                "fastq_list_rows/main": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS"}],
                "fastq_list_rowz": [{'rgid': "YOU_SHOULD_NOT_SEE_THIS_TOO"}]
            }))
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_parse_bcl_convert_output_split_sheets(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_bcl_convert_output_split_sheets
        """

        result = liborca.parse_bcl_convert_output_split_sheets(json.dumps({
            "main/split_sheets": [
                {
                    "location": "gds://umccr-fastq-data/ABCD/SampleSheet.WGS_TsqNano.csv",
                    "basename": "SampleSheet.WGS_TsqNano.csv",
                    "nameroot": "SampleSheet.WGS_TsqNano",
                    "nameext": ".csv",
                    "class": "File",
                    "size": 1394,
                    "http://commonwl.org/cwltool#generation": 0
                },
            ],
            "split_sheets": [{'location': "YOU_SHOULD_NOT_SEE_THIS"}]
        }))

        logger.info("-" * 32)
        logger.info(f"parse_bcl_convert_output_split_sheets: {json.dumps(result)}")

        self.assertEqual(result[0]['location'], "gds://umccr-fastq-data/ABCD/SampleSheet.WGS_TsqNano.csv")

    def test_parse_bcl_convert_output_split_sheets_error(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_bcl_convert_output_split_sheets_error
        """

        try:
            liborca.parse_bcl_convert_output_split_sheets(json.dumps({
                "split_sheets/main": [{'location': "YOU_SHOULD_NOT_SEE_THIS"}],
                "split_sheetz": [{'location': "YOU_SHOULD_NOT_SEE_THIS_TOO"}]
            }))
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(json.JSONDecodeError)

    def test_get_run_number_from_run_name(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_get_run_number_from_run_name
        """
        run_no = liborca.get_run_number_from_run_name(TestConstant.sqr_name.value)
        self.assertEqual(run_no, 1)

    def test_get_timestamp_from_run_name(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_get_timestamp_from_run_name
        """
        run_date = liborca.get_timestamp_from_run_name(TestConstant.sqr_name.value)
        self.assertEqual(run_date, "2020-05-08")

    def test_cwl_file_path_as_string_to_dict(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_cwl_file_path_as_string_to_dict
        """
        result = liborca.cwl_file_path_as_string_to_dict("gds://this/path/to.fastq.gz")
        logger.info(result)
        self.assertTrue(isinstance(result, dict))
        self.assertTrue("class" in result.keys())

    def test_cwl_file_path_as_string_to_dict_alt(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_cwl_file_path_as_string_to_dict_alt
        """
        result = liborca.cwl_file_path_as_string_to_dict({
            'class': "File",
            'location': "gds://this/path/to.fastq.gz"
        })
        logger.info(result)
        self.assertTrue(isinstance(result, dict))
        self.assertTrue("class" in result.keys())

    def test_strip_topup_rerun_from_library_id(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_strip_topup_rerun_from_library_id
        """
        id_in = "L1234567"
        id_out = liborca.strip_topup_rerun_from_library_id(id_in)
        logger.info(f"From {id_in} strip to {id_out}")
        self.assertEqual(id_in, id_out)

        id_in = "L1234567_topup"
        id_out = liborca.strip_topup_rerun_from_library_id(id_in)
        logger.info(f"From {id_in} strip to {id_out}")
        self.assertNotEqual(id_in, id_out)

        id_in = "L1234567_topup2"
        id_out = liborca.strip_topup_rerun_from_library_id(id_in)
        logger.info(f"From {id_in} strip to {id_out}")
        self.assertNotEqual(id_in, id_out)

        id_in = "L1234567_rerun"
        id_out = liborca.strip_topup_rerun_from_library_id(id_in)
        logger.info(f"From {id_in} strip to {id_out}")
        self.assertNotEqual(id_in, id_out)

        id_in = "L1234567_rerun2"
        id_out = liborca.strip_topup_rerun_from_library_id(id_in)
        logger.info(f"From {id_in} strip to {id_out}")
        self.assertNotEqual(id_in, id_out)

    def test_sample_library_id_has_rerun(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_sample_library_id_has_rerun
        """
        id_in = "PRJ123456_L1234567"
        has_rerun = liborca.sample_library_id_has_rerun(id_in)
        logger.info(f"{id_in} has rerun {has_rerun}")
        self.assertFalse(has_rerun)

        id_in = "PRJ123456_LPRJ123456_topup"
        has_rerun = liborca.sample_library_id_has_rerun(id_in)
        logger.info(f"{id_in} has rerun {has_rerun}")
        self.assertFalse(has_rerun)

        id_in = "PRJ123456_LPRJ123456_rerun"
        has_rerun = liborca.sample_library_id_has_rerun(id_in)
        logger.info(f"{id_in} has rerun {has_rerun}")
        self.assertTrue(has_rerun)

        id_in = "PRJ123456_LPRJ123456_rerun2"
        has_rerun = liborca.sample_library_id_has_rerun(id_in)
        logger.info(f"{id_in} has rerun {has_rerun}")
        self.assertTrue(has_rerun)


class LibOrcaIntegrationTests(PipelineIntegrationTestCase):
    # Comment @skip
    # export AWS_PROFILE=dev
    # run the test

    @skip
    def test_get_sample_names_from_samplesheet(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_get_sample_names_from_samplesheet
        """

        # SEQ-II validation dataset
        gds_volume = "umccr-raw-sequence-data-dev"
        samplesheet_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg/SampleSheet.csv"

        sample_names = liborca.get_sample_names_from_samplesheet(
            gds_volume=gds_volume,
            samplesheet_path=samplesheet_path
        )

        self.assertIsNotNone(sample_names)
        self.assertTrue("PTC_SsCRE200323LL_L2000172_topup" in sample_names)

    @skip
    def test_get_samplesheet_to_json(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_get_samplesheet_to_json
        """

        # SEQ-II validation dataset
        gds_volume = "umccr-raw-sequence-data-dev"
        samplesheet_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg/SampleSheet.csv"

        samplesheet_json = liborca.get_samplesheet_to_json(
            gds_volume=gds_volume,
            samplesheet_path=samplesheet_path
        )

        logger.info(samplesheet_json)

        self.assertIsNotNone(samplesheet_json)
        self.assertIsInstance(samplesheet_json, str)
        self.assertNotIsInstance(samplesheet_json, dict)

        logger.info("-" * 32)
        samplesheet_dict = json.loads(samplesheet_json)
        for data_row in samplesheet_dict['Data']:
            if data_row['Sample_ID'] == "PTC_SsCRE200323LL_L2000172_topup":
                logger.info(data_row)
                self.assertEqual(int(data_row['Lane']), 1)
                self.assertEqual(data_row['Sample_Name'], "L2000172_topup")

    @skip
    def test_get_number_of_lanes_from_runinfo(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_get_number_of_lanes_from_runinfo
        """

        # SEQ-II validation dataset
        gds_volume = "umccr-raw-sequence-data-dev"
        runinfo_path = "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg/RunInfo.xml"

        num_lanes = liborca.get_number_of_lanes_from_runinfo(
            gds_volume=gds_volume,
            runinfo_path=runinfo_path
        )

        self.assertEqual(num_lanes, 4)

    @skip
    def test_parse_bcl_convert_output(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_bcl_convert_output
        """

        # SEQ-II bcl conversion in DEV
        wfr_id = "wfr.7e9b649ea780411fb5c87f0e1b0c1923"

        wfl_run = wes.get_run(wfr_id)

        fqlr_from_output = liborca.parse_bcl_convert_output(json.dumps(wfl_run.output))

        logger.info(f"\n{fqlr_from_output}")

        first_one = fqlr_from_output[0]
        self.assertIn("rgid", first_one.keys())

    @skip
    def test_parse_somatic_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_somatic_workflow_output_directory
        """

        # Don't have a good T/N run in DEV yet. So, getting one T/N run from PROD
        wfr_id = "wfr.f472a00d6f45421bb8637e87531e2c66"

        wfl_run = wes.get_run(wfr_id)

        dragen_somatic_output_directory = liborca.parse_somatic_workflow_output_directory(json.dumps(wfl_run.output))

        logger.info(f"\n{dragen_somatic_output_directory}")

        self.assertIn("class", dragen_somatic_output_directory.keys())
        self.assertEqual(dragen_somatic_output_directory['class'], "Directory")

    @skip
    def test_parse_umccrise_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_umccrise_workflow_output_directory
        """

        # umccrise run from PROD
        wfr_id = "wfr.d5c896503d9443ce9579d9f05e75bf71"  # SBJ01312 wgs(L2200063, L2200064)

        wfl_run = wes.get_run(wfr_id)

        umccrise_output_directory = liborca.parse_umccrise_workflow_output_directory(json.dumps(wfl_run.output))

        logger.info(f"\n{umccrise_output_directory}")

        self.assertIn("class", umccrise_output_directory.keys())
        self.assertEqual("Directory", umccrise_output_directory['class'])
        self.assertIn("SBJ01312", umccrise_output_directory['location'])

    @skip
    def test_parse_transcriptome_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_transcriptome_workflow_output_directory
        """

        # wts_tumor_only run from PROD
        wfr_id = "wfr.a9bcb2e3a2fe49b4b64ba01bb2ad9cc9"  # SBJ01312 wts(L2200023)

        wfl_run = wes.get_run(wfr_id)

        dragen_transcriptome_output_directory = liborca.parse_transcriptome_workflow_output_directory(
            json.dumps(wfl_run.output)
        )

        logger.info(f"\n{dragen_transcriptome_output_directory}")

        self.assertIn("class", dragen_transcriptome_output_directory.keys())
        self.assertEqual(dragen_transcriptome_output_directory['class'], "Directory")
        self.assertIn("SBJ01312", dragen_transcriptome_output_directory['location'])
