import json
from unittest import skip

from libica.app import wes
from mockito import when

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
            "output_directory": {
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
        mock_wts_tumor_only_output = json.dumps(
            {
                "arriba_output_directory": {
                    "basename": "arriba",
                    "class": "Directory",
                    "location": "gds://development/analysis_data/SBJ00001/wts_tumor_only/20220312486fec39/arriba",
                    "nameext": "",
                    "nameroot": "arriba",
                    "size": None
                },
                "dragen_transcriptome_output_directory": {
                    "basename": "L3200000_dragen",
                    "class": "Directory",
                    "location": "gds://development/analysis_data/SBJ00001/wts_tumor_only/20220312486fec39/L3200000_dragen",
                    "nameext": "",
                    "nameroot": "L3200000_dragen",
                    "size": None
                },
                "multiqc_output_directory": {
                    "basename": "MDX320000_dragen_transcriptome_multiqc",
                    "class": "Directory",
                    "location": "gds://development/analysis_data/SBJ00001/wts_tumor_only/20220312486fec39/MDX320000_dragen_transcriptome_multiqc",
                    "nameext": "",
                    "nameroot": "MDX320000_dragen_transcriptome_multiqc",
                    "size": None
                },
                "output_dir_gds_folder_id": "fol.cccccccca064e0362d008d89d4636ab",
                "output_dir_gds_session_id": "ssn.99999999b45b4f96bc9baf056a79ede2",
                "somalier_output_directory": {
                    "basename": "MDX320000_somalier",
                    "class": "Directory",
                    "location": "gds://development/analysis_data/SBJ00001/wts_tumor_only/20220312486fec39/MDX320000_somalier",
                    "nameext": "",
                    "nameroot": "MDX320000_somalier",
                    "size": None
                }
            }
        )

        result: dict = liborca.parse_transcriptome_workflow_output_directory(mock_wts_tumor_only_output)
        result_arriba: dict = liborca.parse_arriba_workflow_output_directory(mock_wts_tumor_only_output)

        logger.info("-" * 32)
        logger.info(f"parse_transcriptome_workflow_output_directory: {json.dumps(result)}")
        logger.info(f"parse_arriba_workflow_output_directory: {json.dumps(result_arriba)}")

        self.assertEqual(result['basename'], "L3200000_dragen")
        self.assertEqual(result['class'], "Directory")

        self.assertEqual(result_arriba['basename'], "arriba")
        self.assertEqual(result_arriba['class'], "Directory")

    def test_parse_somatic_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_somatic_workflow_output_directory
        """
        mock_tn_output = json.dumps({
            "dragen_somatic_output_directory": {
                "basename": "L0000002_L0000001_dragen_somatic",
                "class": "Directory",
                "location": "gds://vol/analysis_data/SBJ00001/wgs_tumor_normal/20211208aa4f9099/L0000002_L0000001_dragen_somatic",
                "nameext": "",
                "nameroot": "",
                "size": None
            },
        })
        result: dict = liborca.parse_somatic_workflow_output_directory(mock_tn_output)

        logger.info("-" * 32)
        logger.info(f"parse_somatic_workflow_output_directory: {json.dumps(result)}")

        self.assertEqual(result['basename'], "L0000002_L0000001_dragen_somatic")
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

    def test_parse_germline_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_germline_workflow_output_directory
        """
        mock_tn_output = json.dumps({
            "dragen_germline_output_directory": {
                "basename": "L0000001_dragen_germline",
                "class": "Directory",
                "location": "gds://vol/analysis_data/SBJ00001/wgs_tumor_normal/20211208aa4f9099/L0000001_dragen_germline",
                "nameext": "",
                "nameroot": "",
                "size": None
            },
        })
        result: dict = liborca.parse_germline_workflow_output_directory(mock_tn_output)

        logger.info("-" * 32)
        logger.info(f"parse_germline_workflow_output_directory: {json.dumps(result)}")

        self.assertEqual(result['basename'], "L0000001_dragen_germline")
        self.assertEqual(result['class'], "Directory")

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

    def test_parse_wgs_alignment_qc_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_wgs_alignment_qc_output_for_bam_file
        """
        dragen_bam_out = liborca.parse_wgs_alignment_qc_output_for_bam_file(json.dumps({
            "dragen_bam_out": {
                "location": "gds://vol/analysis_data/SBJ00001/wgs_alignment_qc/2022052276d4397b/L3200006__3_dragen/PRJ320001.bam",
                "basename": "PRJ320001.bam",
                "nameroot": "PRJ320001",
                "nameext": ".bam",
                "class": "File",
                "size": 44992201152,
                "secondaryFiles": [
                    {
                        "basename": "PRJ320001.bam.bai",
                        "location": "gds://vol/analysis_data/SBJ00001/wgs_alignment_qc/2022052276d4397b/L3200006__3_dragen/PRJ320001.bam.bai",
                        "class": "File",
                        "nameroot": "PRJ320001.bam",
                        "nameext": ".bai",
                        "http://commonwl.org/cwltool#generation": 0
                    }
                ],
                "http://commonwl.org/cwltool#generation": 0
            },
        }))

        self.assertRegex(dragen_bam_out, r"^gds:\/\/\S+\.bam$")
        logger.info(dragen_bam_out)

    def test_parse_wts_alignment_qc_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_wts_alignment_qc_output_for_bam_file
        """
        dragen_bam_out = liborca.parse_wgs_alignment_qc_output_for_bam_file(json.dumps(
            {
                "dragen_alignment_output_directory": {
                    "basename": "L4100001__1_dragen",
                    "class": "Directory",
                    "location": "gds://development/temp/dragen_alignment_4_2_4/output/20230808_170056/L4100001__1_dragen",
                    "nameext": "",
                    "nameroot": "L4100001__1_dragen",
                    "size": None
                },
                "dragen_bam_out": {
                    "basename": "PTC_NebRNA111111.bam",
                    "class": "File",
                    "http://commonwl.org/cwltool#generation": 0,
                    "location": "gds://development/temp/dragen_alignment_4_2_4/output/20230808_170056/L4100001__1_dragen/PTC_NebRNA111111.bam",
                    "nameext": ".bam",
                    "nameroot": "PTC_NebRNA111111",
                    "secondaryFiles": [
                        {
                            "basename": "PTC_NebRNA111111.bam.bai",
                            "class": "File",
                            "http://commonwl.org/cwltool#generation": 0,
                            "location": "gds://development/temp/dragen_alignment_4_2_4/output/20230808_170056/L4100001__1_dragen/PTC_NebRNA111111.bam.bai",
                            "nameext": ".bai",
                            "nameroot": "PTC_NebRNA111111.bam"
                        }
                    ],
                    "size": 1439711974
                },
                "multiqc_output_directory": {
                    "basename": "PTC_NebRNA111111_dragen_alignment_multiqc",
                    "class": "Directory",
                    "location": "gds://development/temp/dragen_alignment_4_2_4/output/20230808_170056/PTC_NebRNA111111_dragen_alignment_multiqc",
                    "nameext": "",
                    "nameroot": "PTC_NebRNA111111_dragen_alignment_multiqc",
                    "size": None
                },
                "output_dir_gds_folder_id": "fol.123",
                "output_dir_gds_session_id": "ssn.321"
            }
        ))

        self.assertRegex(dragen_bam_out, r"^gds:\/\/\S+\.bam$")
        logger.info(dragen_bam_out)

    def test_parse_transcriptome_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_transcriptome_output_for_bam_file
        """
        when(liborca.gds).get_files_from_gds_by_suffix(...).thenReturn(["gds://vol/fol/L4200002_dragen.bam", ])

        transcriptome_bam = liborca.parse_transcriptome_output_for_bam_file(json.dumps({
            "dragen_transcriptome_output_directory": {
                "location": "gds://vol/analysis_data/SBJ00009/wts_tumor_only/202205155e5fec36/L4200002_dragen",
                "basename": "L4200002_dragen",
                "nameroot": "L4200002_dragen",
                "nameext": "",
                "class": "Directory",
                "size": None
            },
        }))

        self.assertRegex(transcriptome_bam, r"^gds:\/\/\S+\.bam$")
        logger.info(transcriptome_bam)

    def test_parse_tso_ctdna_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_tso_ctdna_output_for_bam_file
        """
        # NOTE: we would not really need a big mock here but that's the intention
        # so that we capture the workflow output structure at this point
        # When this "contract" is changed, it is unittest job to determine this contract is still check-in-place

        when(liborca.gds).check_file(...).thenReturn(["gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results/PRJ420001_L4200001/PRJ420001_L4200001.bam", ])

        tso_ctdna_bam = liborca.parse_tso_ctdna_output_for_bam_file(json.dumps(
            {
                "output_results_dir": {
                    "location": "gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results",
                    "basename": "Results",
                    "nameroot": "Results",
                    "nameext": "",
                    "class": "Directory",
                    "size": None
                },
                "output_results_dir_by_sample": [
                    {
                        "class": "Directory",
                        "location": "gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results/PRJ420001_L4200001",
                        "basename": "PRJ420001_L4200001",
                        "listing": [
                            {
                                "class": "File",
                                "location": "gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results/PRJ420001_L4200001/PRJ420001_L4200001.AlignCollapseFusionCaller_metrics.json.gz",
                                "basename": "PRJ420001_L4200001.AlignCollapseFusionCaller_metrics.json.gz",
                                "size": 2798,
                                "nameroot": "PRJ420001_L4200001.AlignCollapseFusionCaller_metrics.json",
                                "nameext": ".gz"
                            },
                            {
                                "class": "File",
                                "location": "gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results/PRJ420001_L4200001/PRJ420001_L4200001.bam",
                                "basename": "PRJ420001_L4200001.bam",
                                "size": 7714127545,
                                "nameroot": "PRJ420001_L4200001",
                                "nameext": ".bam"
                            },
                            {
                                "class": "File",
                                "location": "gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results/PRJ420001_L4200001/PRJ420001_L4200001.cleaned.stitched.bam",
                                "basename": "PRJ420001_L4200001.cleaned.stitched.bam",
                                "size": 3423990554,
                                "nameroot": "PRJ420001_L4200001.cleaned.stitched",
                                "nameext": ".bam"
                            },
                            {
                                "class": "File",
                                "location": "gds://vol/analysis_data/SBJ00001/tso_ctdna_tumor_only/20221011820647e4/L4200001/Results/PRJ420001_L4200001/evidence.PRJ420001_L4200001.bam",
                                "basename": "evidence.PRJ420001_L4200001.bam",
                                "size": 5336845,
                                "nameroot": "evidence.PRJ420001_L4200001",
                                "nameext": ".bam"
                            },
                        ]
                    }
                ],
            }
        ))

        self.assertRegex(tso_ctdna_bam, r"^gds:\/\/\S+\.bam$")
        logger.info(tso_ctdna_bam)

    def test_parse_wgs_tumor_normal_output_for_bam_files(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_wgs_tumor_normal_output_for_bam_files
        """
        wgs_tumor_normal_output = {
            "normal_bam_out": {
                "location": "gds://volume/path/normal.bam",
                "basename": "normal.bam",
                "nameroot": "normal",
                "nameext": ".bam",
                "class": "File",
                "size": 98432974542,
                "secondaryFiles": [
                    {
                        "basename": "normal.bam.bai",
                        "location": "gds://volume/path/normal.bam.bai",
                        "class": "File",
                        "nameroot": "normal.bam",
                        "nameext": ".bai",
                        "http://commonwl.org/cwltool#generation": 0,
                        "size": 9892168
                    }
                ],
                "http://commonwl.org/cwltool#generation": 0
            },
            "tumor_bam_out": {
                "location": "gds://volume/path/tumor.bam",
                "basename": "tumor.bam",
                "nameroot": "tumor",
                "nameext": ".bam",
                "class": "File",
                "size": 183053246790,
                "secondaryFiles": [
                    {
                        "basename": "tumor.bam.bai",
                        "location": "gds://volume/path/tumor.bam.bai",
                        "class": "File",
                        "nameroot": "tumor.bam",
                        "nameext": ".bai",
                        "http://commonwl.org/cwltool#generation": 0
                    }
                ],
                "http://commonwl.org/cwltool#generation": 0
            }
        }

        tumor_bam_out, normal_bam_out = liborca.parse_wgs_tumor_normal_output_for_bam_files(json.dumps(wgs_tumor_normal_output))
        self.assertRegex(tumor_bam_out, r"^gds:\/\/\S+\.bam$")
        self.assertRegex(normal_bam_out, r"^gds:\/\/\S+\.bam$")
        logger.info(tumor_bam_out)
        logger.info(normal_bam_out)

    def test_parse_portal_run_id_from_path_element(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_portal_run_id_from_path_element
        """
        portal_run_id = liborca.parse_portal_run_id_from_path_element("gds://vol1////path/20231010abcdefg1/some.bam")
        logger.info(portal_run_id)
        self.assertEqual(portal_run_id, "20231010abcdefg1")

    def test_parse_portal_run_id_from_path_element_raise(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_portal_run_id_from_path_element_raise
        """
        with self.assertRaises(AssertionError) as cm:
            _ = liborca.parse_portal_run_id_from_path_element("gds://vol1//20231010abcdefg1//path/20231010abcdefg1/some.bam")
        e = cm.exception
        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Malformed path", str(e))

    def test_parse_oncoanalyser_workflow_output_directory(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_oncoanalyser_workflow_output_directory
        """
        mock_uri = "s3://bk1/key/down/under"
        out_dir = liborca.parse_oncoanalyser_workflow_output_directory(json.dumps({
            'output_directory': mock_uri
        }))
        logger.info(out_dir)
        self.assertEqual(out_dir, mock_uri)

    def test_parse_oncoanalyser_workflow_output_directory_raise1(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_oncoanalyser_workflow_output_directory_raise1
        """
        mock_uri = "s3://bk1/key/down/under"
        with self.assertRaises(ValueError) as cm:
            _ = liborca.parse_oncoanalyser_workflow_output_directory(json.dumps({
                'output_directory': {
                    'class': "Directory",
                    'location': mock_uri
                }
            }))
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("oncoanalyser output is not string", str(e))

    def test_parse_oncoanalyser_workflow_output_directory_raise2(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaUnitTests.test_parse_oncoanalyser_workflow_output_directory_raise2
        """
        mock_uri = "s3://bk1/key/down/under"
        with self.assertRaises(KeyError) as cm:
            _ = liborca.parse_oncoanalyser_workflow_output_directory(json.dumps({
                'output_directory1': mock_uri
            }))
        e = cm.exception

        logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{str(e)}")
        self.assertIn("Unexpected workflow output format", str(e))


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
        unset ICA_ACCESS_TOKEN
        export AWS_PROFILE=prod
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
    def test_parse_germline_workflow_output_directory(self):
        """
        unset ICA_ACCESS_TOKEN
        export AWS_PROFILE=dev
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_germline_workflow_output_directory
        """

        wfr_id = "wfr.5616c72c82e442f78f0f9f0d6441219e"  # in DEV

        wfl_run = wes.get_run(wfr_id)

        dragen_germline_output_directory = liborca.parse_germline_workflow_output_directory(json.dumps(wfl_run.output))

        logger.info(f"\n{dragen_germline_output_directory}")

        self.assertIn("class", dragen_germline_output_directory.keys())
        self.assertEqual(dragen_germline_output_directory['class'], "Directory")

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

        # wts_tumor_only run from DEV, see https://umccr.slack.com/archives/C7QC9N8G4/p1647112587377499
        wfr_id = "wfr.b61f86b3ac2748fe997ecdf1d4b79d84"  # L2100732

        wfl_run = wes.get_run(wfr_id)

        dragen_transcriptome_output_directory = liborca.parse_transcriptome_workflow_output_directory(
            json.dumps(wfl_run.output)
        )

        arriba_output_directory = liborca.parse_arriba_workflow_output_directory(json.dumps(wfl_run.output))

        logger.info(f"\n{dragen_transcriptome_output_directory}")
        logger.info(f"\n{arriba_output_directory}")

        self.assertIn("class", dragen_transcriptome_output_directory.keys())
        self.assertEqual(dragen_transcriptome_output_directory['class'], "Directory")
        self.assertIn("SBJ00910", dragen_transcriptome_output_directory['location'])

        self.assertIn("class", arriba_output_directory.keys())
        self.assertEqual(arriba_output_directory['class'], "Directory")
        self.assertIn("SBJ00910", arriba_output_directory['location'])
        self.assertIn("arriba", arriba_output_directory['location'])

    @skip
    def test_parse_wgs_alignment_qc_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_wgs_alignment_qc_output_for_bam_file
        """

        wfr_id = "wfr.0d3dea278b1c471d8316b9d5a242dd34"  # SBJ00913

        wfl_run = wes.get_run(wfr_id)

        # First assure that this IS a dragen wgs qc workflow
        dragen_wgs_alignment_output_directory = liborca.parse_workflow_output(
            json.dumps(wfl_run.output), ["dragen_alignment_output_directory"]
        )
        self.assertIn("class", dragen_wgs_alignment_output_directory.keys())
        self.assertEqual(dragen_wgs_alignment_output_directory['class'], "Directory")

        # Then assert this is a transcriptome bam file
        dragen_bam_out = liborca.parse_wgs_alignment_qc_output_for_bam_file(
            json.dumps(wfl_run.output)
        )
        # Assert that we return a bam file
        self.assertRegex(dragen_bam_out, r"^gds:\/\/\S+\.bam$")
        logger.info(dragen_bam_out)

    @skip
    def test_parse_transcriptome_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_transcriptome_output_for_bam_file
        """

        wfr_id = "wfr.b61f86b3ac2748fe997ecdf1d4b79d84"  # SBJ00910__L2100732

        wfl_run = wes.get_run(wfr_id)

        # First assure that this IS a dragen transcriptome workflow
        dragen_transcriptome_output_directory = liborca.parse_transcriptome_workflow_output_directory(
            json.dumps(wfl_run.output)
        )
        self.assertIn("class", dragen_transcriptome_output_directory.keys())
        self.assertEqual(dragen_transcriptome_output_directory['class'], "Directory")

        # Then assert this is a transcriptome bam file
        dragen_transcriptome_bam_file = liborca.parse_transcriptome_output_for_bam_file(
            json.dumps(wfl_run.output)
        )
        # Assert that we return a bam file
        self.assertRegex(dragen_transcriptome_bam_file, r"^gds:\/\/\S+\.bam$")
        logger.info(dragen_transcriptome_bam_file)

    @skip
    def test_parse_tso_ctdna_output_for_bam_file(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_tso_ctdna_output_for_bam_file
        """

        wfr_id = "wfr.bee03c85c5c44d63925b35428ef147a2"  # SBJ02873 in PROD

        wfl_run = wes.get_run(wfr_id)

        dragen_tso_ctdna_main_bam_file = liborca.parse_tso_ctdna_output_for_bam_file(
            json.dumps(wfl_run.output)
        )

        # Assert that we return a bam file
        self.assertRegex(dragen_tso_ctdna_main_bam_file, r"^gds:\/\/\S+\.bam$")
        self.assertIn("SBJ02873", dragen_tso_ctdna_main_bam_file)
        logger.info(dragen_tso_ctdna_main_bam_file)

    @skip
    def test_parse_wgs_tumor_normal_output_for_bam_files(self):
        """
        python manage.py test data_processors.pipeline.tools.tests.test_liborca.LibOrcaIntegrationTests.test_parse_wgs_tumor_normal_output_for_bam_files
        """

        wfr_id = "wfr.22ed619250e14a449fff8ac7bc9d41c0"  # SBJ04375 in PROD

        wfl_run = wes.get_run(wfr_id)

        tumor_bam_out, normal_bam_out = liborca.parse_wgs_tumor_normal_output_for_bam_files(
            json.dumps(wfl_run.output)
        )

        # Assert that we return a bam file
        self.assertRegex(tumor_bam_out, r"^gds:\/\/\S+\.bam$")
        self.assertRegex(normal_bam_out, r"^gds:\/\/\S+\.bam$")
        self.assertIn("SBJ04375", tumor_bam_out)
        logger.info(tumor_bam_out)
        logger.info(normal_bam_out)
