from data_portal.models.report import ReportType
from data_processors.reports.services import gds_report_srv
from data_processors.reports.tests.case import ReportUnitTestCase, logger

PATH_EXPECTED = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"

PATH_EXPECTED_ALT_1 = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/FILENAME__DOES_NOT_MATTER-metrics.json.gz"
PATH_EXPECTED_ALT_2 = "/analysis_data/FOLDER_LEVEL_DOES_NOT_MATTER/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/FILENAME__DOES_NOT_MATTER-metrics.json.gz"
PATH_EXPECTED_ALT_3 = "/analysis_data/SBJ00001/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"
PATH_EXPECTED_ALT_4 = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"
PATH_EXPECTED_ALT_5 = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001_rerun/PRJ000001_L0000001_rerun.TargetRegionCoverage.json.gz"
PATH_EXPECTED_ALT_6 = "/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001_topup/PRJ000001_L0000001_topup.AlignCollapseFusionCaller_metrics.json.gz"
PATH_EXPECTED_ALT_7 = "gds://development/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001.fragment_length_hist.json"

PATH_EXPECTED_ALL = [
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345.AlignCollapseFusionCaller_metrics.json.gz", ReportType.FUSION_CALLER_METRICS),
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345.msi.json.gz", ReportType.MSI),
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345.TargetRegionCoverage.json.gz", ReportType.TARGET_REGION_COVERAGE),
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345.tmb.json.gz", ReportType.TMB),
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345_Failed_Exon_coverage_QC.json.gz", ReportType.FAILED_EXON_COVERAGE_QC),
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345_SampleAnalysisResults.json.gz", ReportType.SAMPLE_ANALYSIS_RESULTS),
    ("/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345_TMB_Trace.json.gz", ReportType.TMB_TRACE),
]

PATH_ERROR_1 = "/SBI00001/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"
PATH_ERROR_2 = "/SBJ00001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"
PATH_ERROR_3 = "/SBJ00001/PRJ000001_L0000001/PRJ000001_L0000001_Failed_Exxon_coverage_QC.json.gz"


class ReportUniqueKeyUnitTests(ReportUnitTestCase):

    def test_extract_report_unique_key(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_1(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_1

        NOTE:
            Alternate test to assert that file name does not matter
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_1)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_2(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_2

        NOTE:
            Alternate test to assert that folder name does not matter
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_2)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_3(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_3

        NOTE:
            Alternate test to assert that folder name does not matter
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_3)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_4(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_4

        NOTE:
            Alternate test to assert that folder name does not matter
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_4)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertEqual(subject_id, "SBJ00001")
        self.assertEqual(sample_id, "PRJ000001")
        self.assertEqual(library_id, "L0000001")

    def test_extract_report_unique_key_alt_5(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_5
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_5)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, PATH_EXPECTED_ALT_5)
        self.assertIn(sample_id, PATH_EXPECTED_ALT_5)
        self.assertIn(library_id, PATH_EXPECTED_ALT_5)
        self.assertIn("rerun", library_id)

    def test_extract_report_unique_key_alt_6(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_6
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_6)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, PATH_EXPECTED_ALT_6)
        self.assertIn(sample_id, PATH_EXPECTED_ALT_6)
        self.assertIn(library_id, PATH_EXPECTED_ALT_6)
        self.assertIn("topup", library_id)

    def test_extract_report_unique_key_alt_7(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_alt_7
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_EXPECTED_ALT_7)
        logger.info(f"SUBJECT_ID: {subject_id}, SAMPLE_ID: {sample_id}, LIBRARY_ID: {library_id}")
        self.assertIn(subject_id, PATH_EXPECTED_ALT_7)
        self.assertIn(sample_id, PATH_EXPECTED_ALT_7)
        self.assertIn(library_id, PATH_EXPECTED_ALT_7)

    def test_extract_report_unique_key_error_1(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_error_1
        """
        subject_id, sample_id, library_id = gds_report_srv._extract_report_unique_key(PATH_ERROR_1)
        self.assertIsNone(subject_id)

    def test_extract_report_unique_key_error_2(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_unique_key_error_2
        """
        subject_id, _, _ = gds_report_srv._extract_report_unique_key(PATH_ERROR_2)
        self.assertIsNone(subject_id)

    def test_extract_report_type(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_type
        """
        type_ = gds_report_srv._extract_report_type(PATH_EXPECTED)
        logger.info(f"ReportType: {type_}")
        self.assertEqual(type_, ReportType.FUSION_CALLER_METRICS)

    def test_extract_report_type_check_all(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_type_check_all
        """
        chk = 0
        for k, t in PATH_EXPECTED_ALL:
            type_ = gds_report_srv._extract_report_type(k)
            logger.info(f"[Check] ReportType: {type_}")
            self.assertEqual(type_, t)
            chk += 1
        self.assertEqual(chk, len(PATH_EXPECTED_ALL))

    def test_extract_report_type_unknown(self):
        """
        python manage.py test data_processors.reports.tests.test_report_uk_gds.ReportUniqueKeyUnitTests.test_extract_report_type_unknown
        """
        type_ = gds_report_srv._extract_report_type(PATH_ERROR_3)
        self.assertIsNone(type_)
