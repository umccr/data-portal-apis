from data_processors.reports import services
from data_processors.reports.tests.case import ReportUnitTestCase, ReportIntegrationTestCase, logger

KEY_EXPECTED = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/SBJ00001__SBJ00001_PRJ000001_L0000001/cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"

KEY_EXPECTED_ALT_1 = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/SBJ00001__SBJ00001_PRJ000001_L0000001/cancer_report_tables/json/hrd/FILENAME__DOES_NOT_MATTER-hrdetect.json.gz"
KEY_EXPECTED_ALT_2 = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/FOLDERNAME__DOES_NOT_MATTER/cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_EXPECTED_ALT_3 = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/SBJ0000O__SBJ00001_PRJ000001_L0000001/cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_EXPECTED_ALT_4 = "cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_EXPECTED_ALT_5 = "cancer_report_tables/json/hrd/SBJ66666__SBJ66666_MDX888888_L9999999_rerun-qc_summary.json.gz"
KEY_EXPECTED_ALT_6 = "cancer_report_tables/json/hrd/SBJ66666__SBJ66666_MDX888888_L9999999_topup-qc_summary.json.gz"

KEY_ERROR_1 = "cancer_report_tables/json/hrd/SBJ00001__SBI00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_ERROR_2 = "cancer_report_tables/json/hrd/SBJ00001/hrdetect.json.gz"


class ReportUniqueKeyUnitTests(ReportUnitTestCase):

    def test_extract_report_unique_key(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_1(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_1

        NOTE:
            Alternate test to assert that file name does not matter
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED_ALT_1)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_2(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_2

        NOTE:
            Alternate test to assert that folder name does not matter
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED_ALT_2)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_3(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_3

        NOTE:
            Alternate test to assert that folder name does not matter
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED_ALT_2)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_4(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_4

        NOTE:
            Alternate test to assert that folder name does not matter
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED_ALT_4)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_5(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_5
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED_ALT_5)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, KEY_EXPECTED_ALT_5)
        self.assertIn(sample_id, KEY_EXPECTED_ALT_5)
        self.assertIn(library_id, KEY_EXPECTED_ALT_5)
        self.assertIn("rerun", library_id)

    def test_extract_report_unique_key_alt_6(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_6
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_EXPECTED_ALT_6)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, KEY_EXPECTED_ALT_6)
        self.assertIn(sample_id, KEY_EXPECTED_ALT_6)
        self.assertIn(library_id, KEY_EXPECTED_ALT_6)
        self.assertIn("topup", library_id)

    def test_extract_report_unique_key_error_1(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_error_1
        """
        subject_id, sample_id, library_id = services._extract_report_unique_key(KEY_ERROR_1)
        self.assertIsNone(subject_id)

    def test_extract_report_unique_key_error_2(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_error_2
        """
        subject_id, _, _ = services._extract_report_unique_key(KEY_ERROR_2)
        self.assertIsNone(subject_id)


class ReportUniqueKeyIntegrationTests(ReportIntegrationTestCase):
    pass
