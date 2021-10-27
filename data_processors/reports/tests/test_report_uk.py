from data_portal.models.report import ReportType
from data_processors.reports.services import s3_report_srv
from data_processors.reports.tests.case import ReportUnitTestCase, ReportIntegrationTestCase, logger

KEY_EXPECTED = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/SBJ00001__SBJ00001_PRJ000001_L0000001/cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"

KEY_EXPECTED_ALT_1 = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/SBJ00001__SBJ00001_PRJ000001_L0000001/cancer_report_tables/json/hrd/FILENAME__DOES_NOT_MATTER-hrdetect.json.gz"
KEY_EXPECTED_ALT_2 = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/FOLDERNAME__DOES_NOT_MATTER/cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_EXPECTED_ALT_3 = "Project-CUP/SBJ00001/WGS/2020-12-07/umccrised/SBJ0000O__SBJ00001_PRJ000001_L0000001/cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_EXPECTED_ALT_4 = "cancer_report_tables/json/hrd/SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_EXPECTED_ALT_5 = "cancer_report_tables/json/hrd/SBJ66666__SBJ66666_MDX888888_L9999999_rerun-qc_summary.json.gz"
KEY_EXPECTED_ALT_6 = "cancer_report_tables/json/hrd/SBJ66666__SBJ66666_MDX888888_L9999999_topup-qc_summary.json.gz"
KEY_EXPECTED_ALT_7 = "SBJ00742__SBJ00742_PRJ210259_L2100263/SBJ00742__SBJ00742_PRJ210259_L2100263-multiqc_report_data/multiqc_data.json"


KEY_EXPECTED_ALL = [
    ("SBJ00742__SBJ00742_PRJ210259_L2100263-multiqc_report_data/multiqc_data.json", ReportType.MULTIQC),
    ("SBJ00001__SBJ00001_MDX000001_L0000001-qc_summary.json.gz", ReportType.QC_SUMMARY),
    ("SBJ00001__SBJ00001_MDX000001_L0000001-report_inputs.json.gz", ReportType.REPORT_INPUTS),
    ("hrd/SBJ00001__SBJ00001_MDX000001_L0000001-chord.json.gz", ReportType.HRD_CHORD),
    ("hrd/SBJ00001__SBJ00001_MDX000001_L0000001-hrdetect.json.gz", ReportType.HRD_HRDETECT),
    ("purple/SBJ00001__SBJ00001_MDX000001_L0000001-purple_cnv_germ.json.gz", ReportType.PURPLE_CNV_GERM),
    ("purple/SBJ00001__SBJ00001_MDX000001_L0000001-purple_cnv_som.json.gz", ReportType.PURPLE_CNV_SOM),
    ("purple/SBJ00001__SBJ00001_MDX000001_L0000001-purple_cnv_som_gene.json.gz", ReportType.PURPLE_CNV_SOM_GENE),
    ("sigs/SBJ00001__SBJ00001_MDX000001_L0000001-indel.json.gz", ReportType.SIGS_INDEL),
    ("sigs/SBJ00001__SBJ00001_MDX000001_L0000001-snv_2015.json.gz", ReportType.SIGS_SNV_2015),
    ("sigs/SBJ00001__SBJ00001_MDX000001_L0000001-snv_2020.json.gz", ReportType.SIGS_SNV_2020),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-01_sv_unmelted.json.gz", ReportType.SV_UNMELTED),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-02_sv_melted.json.gz", ReportType.SV_MELTED),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-03_sv_BND_main.json.gz", ReportType.SV_BND_MAIN),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-04_sv_BND_purpleinf.json.gz", ReportType.SV_BND_PURPLEINF),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-05_sv_noBND_main.json.gz", ReportType.SV_NOBND_MAIN),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-06_sv_noBND_other.json.gz", ReportType.SV_NOBND_OTHER),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-07_sv_noBND_manygenes.json.gz", ReportType.SV_NOBND_MANYGENES),
    ("sv/SBJ00001__SBJ00001_MDX000001_L0000001-08_sv_noBND_manytranscripts.json.gz", ReportType.SV_NOBND_MANYTRANSCRIPTS),
]

KEY_ERROR_1 = "cancer_report_tables/json/hrd/SBJ00001__SBI00001_PRJ000001_L0000001-hrdetect.json.gz"
KEY_ERROR_2 = "cancer_report_tables/json/hrd/SBJ00001/hrdetect.json.gz"
KEY_ERROR_3 = "cancer_report_tables/json/hrd/SBJ00001/hrddetect.json.gz"


class ReportUniqueKeyUnitTests(ReportUnitTestCase):

    def test_extract_report_unique_key(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key
        """
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED)
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
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_1)
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
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_2)
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
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_3)
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
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_4)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_5(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_5
        """
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_5)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, KEY_EXPECTED_ALT_5)
        self.assertIn(sample_id, KEY_EXPECTED_ALT_5)
        self.assertIn(library_id, KEY_EXPECTED_ALT_5)
        self.assertIn("rerun", library_id)

    def test_extract_report_unique_key_alt_6(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_6
        """
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_6)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, KEY_EXPECTED_ALT_6)
        self.assertIn(sample_id, KEY_EXPECTED_ALT_6)
        self.assertIn(library_id, KEY_EXPECTED_ALT_6)
        self.assertIn("topup", library_id)

    def test_extract_report_unique_key_alt_7(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_7
        """
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_EXPECTED_ALT_7)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, KEY_EXPECTED_ALT_7)
        self.assertIn(sample_id, KEY_EXPECTED_ALT_7)
        self.assertIn(library_id, KEY_EXPECTED_ALT_7)

    def test_extract_report_unique_key_error_1(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_error_1
        """
        subject_id, sample_id, library_id = s3_report_srv._extract_report_unique_key(KEY_ERROR_1)
        self.assertIsNone(subject_id)

    def test_extract_report_unique_key_error_2(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_unique_key_error_2
        """
        subject_id, _, _ = s3_report_srv._extract_report_unique_key(KEY_ERROR_2)
        self.assertIsNone(subject_id)

    def test_extract_report_type(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_type
        """
        type_ = s3_report_srv._extract_report_type(KEY_EXPECTED)
        self.assertEqual(type_, ReportType.HRD_HRDETECT)

    def test_extract_report_type_check_all(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_type_check_all
        """
        chk = 0
        for k, t in KEY_EXPECTED_ALL:
            type_ = s3_report_srv._extract_report_type(k)
            self.assertEqual(type_, t)
            chk += 1
        self.assertEqual(chk, len(KEY_EXPECTED_ALL))

    def test_extract_report_type_unknown(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk.ReportUniqueKeyUnitTests.test_extract_report_type_unknown
        """
        type_ = s3_report_srv._extract_report_type(KEY_ERROR_3)
        self.assertIsNone(type_)


class ReportUniqueKeyIntegrationTests(ReportIntegrationTestCase):
    pass
