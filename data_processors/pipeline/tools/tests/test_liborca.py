import json

from data_portal.tests.factories import TestConstant
from data_processors.pipeline.tools import liborca
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class LibOrcaUnitTests(PipelineUnitTestCase):

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