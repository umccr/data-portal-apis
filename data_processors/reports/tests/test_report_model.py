from django.db.models import QuerySet

from data_portal.models import Report, ReportType
from data_portal.tests import factories
from data_processors.reports.tests.case import ReportUnitTestCase, ReportIntegrationTestCase, logger


class ReportModelUnitTests(ReportUnitTestCase):

    def test_umccrise_report_model(self):
        """
        python manage.py test data_processors.reports.tests.test_report_model.ReportModelUnitTests.test_umccrise_report_model
        """

        hrd_chord: Report = factories.HRDChordReportFactory()
        hrd_hrdetect: Report = factories.HRDetectReportFactory()
        purple_cnv_germ: Report = factories.PurpleCNVGermReportFactory()
        purple_cnv_som: Report = factories.PurpleCNVSomReportFactory()
        purple_cnv_som_gene: Report = factories.PurpleCNVSomGeneReportFactory()
        sigs_dbs: Report = factories.SigsDBSReportFactory()
        sigs_indel: Report = factories.SigsIndelReportFactory()
        sigs_snv_2015: Report = factories.SigsSNV2015ReportFactory()
        sigs_snv_2020: Report = factories.SigsSNV2020ReportFactory()
        sv_unmelted: Report = factories.SvUnmeltedReportFactory()
        sv_melted: Report = factories.SvMeltedReportFactory()
        sv_bnd_main: Report = factories.SvBNDMainReportFactory()
        sv_bnd_purpleinf: Report = factories.SvBNDPurpleinfReportFactory()
        sv_nobnd_main: Report = factories.SvNoBNDMainReportFactory()
        sv_nobnd_other: Report = factories.SvNoBNDOtherReportFactory()
        sv_nobnd_manygenes: Report = factories.SvNoBNDManyGenesReportFactory()
        sv_nobnd_manytranscripts: Report = factories.SvNoBNDManyTranscriptsReportFactory()

        logger.info(f"hrd_chord: {hrd_chord}")
        logger.info(f"hrd_hrdetect: {hrd_hrdetect}")
        logger.info(f"purple_cnv_germ: {purple_cnv_germ}")
        logger.info(f"purple_cnv_som: {purple_cnv_som}")
        logger.info(f"purple_cnv_som_gene: {purple_cnv_som_gene}")
        logger.info(f"sigs_dbs: {sigs_dbs}")
        logger.info(f"sigs_indel: {sigs_indel}")
        logger.info(f"sigs_snv_2015: {sigs_snv_2015}")
        logger.info(f"sigs_snv_2020: {sigs_snv_2020}")
        logger.info(f"sv_unmelted: {sv_unmelted}")
        logger.info(f"sv_melted: {sv_melted}")
        logger.info(f"sv_bnd_main: {sv_bnd_main}")
        logger.info(f"sv_bnd_purpleinf: {sv_bnd_purpleinf}")
        logger.info(f"sv_nobnd_main: {sv_nobnd_main}")
        logger.info(f"sv_nobnd_other: {sv_nobnd_other}")
        logger.info(f"sv_nobnd_manygenes: {sv_nobnd_manygenes}")
        logger.info(f"sv_nobnd_manytranscripts: {sv_nobnd_manytranscripts}")

        unknown_report = factories.ReportFactory()
        logger.info(f"unknown_report: {unknown_report}")
        self.assertIsNone(unknown_report.data)

    def test_sv_melted_report_filter(self):
        """export DJANGO_SETTINGS_MODULE=data_portal.settings.local
        python manage.py test data_processors.reports.tests.test_report_model.ReportModelUnitTests.test_sv_melted_report_filter

        NOTE:
        If you are seeing this error:
            django.db.utils.NotSupportedError: contains lookup is not supported on this database backend.

        Make sure to run against local setting i.e.
            export DJANGO_SETTINGS_MODULE=data_portal.settings.local

        Reason: JSONField query do require MySQL database
        """

        # populate SvMeltedReport fixture in db
        factories.SvMeltedReportFactory()

        # check in db, should have 1 report record
        self.assertEqual(1, Report.objects.count())

        # try report filter query on data JSONField where `nann` is 3, should be 0 match
        qs: QuerySet = Report.objects.filter(data__contains=[{"nann": 3}])
        self.assertEqual(0, qs.count())

    def test_create_report(self):
        """
        python manage.py test data_processors.reports.tests.test_report_model.ReportModelUnitTests.test_create_report
        """

        mock_data = [
            {
                "Chr": "chr1",
                "Start": 1,
                "End": 7196377,
                "CN": 0.7,
                "CN Min+Maj": "0+0.7",
                "Start/End SegSupport": "TELOMERE-BND",
                "Method": "BAF_WEIGHTED",
                "BAF (count)": "1 (6)",
                "GC (windowCount)": "0.52 (4515)"
            }
        ]

        # instantiate a Report
        report = Report(
            subject_id="SBJ00001",
            sample_id="MDX111111",
            library_id="L12345678",
            type=ReportType.PURPLE_CNV_SOM,
            created_by="me",
            data=mock_data,
        )

        # save it to db
        report.save()

        # check that there should be 1 report record in db
        self.assertEqual(1, Report.objects.count())

        # query back: get report(s) by sample id
        qs: QuerySet = Report.objects.filter(sample_id="MDX111111")

        # simply qs.get() it since we know that there is only 1 record, otherwise should qs.all()
        # and iterate all matched records for query `sample_id` condition
        r: Report = qs.get()

        logger.info(r)
        logger.info(r.data)
        logger.info(type(r.data))

        # iterate report's data points and show `CN` value
        for d in r.data:
            logger.info(d.get('CN'))


class ReportModelIntegrationTests(ReportIntegrationTestCase):
    pass
