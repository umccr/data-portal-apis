import logging

from django.db.models import QuerySet
from django.test import override_settings

from data_portal.models import Report, ReportType, S3Object
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
            sample_id="MDX000001",
            library_id="L0000001",
            type=ReportType.PURPLE_CNV_SOM,
            created_by="me",
            data=mock_data,
        )

        # save it to db
        report.save()

        # check that there should be 1 report record in db
        self.assertEqual(1, Report.objects.count())

        # query back: get report(s) by sample id
        qs: QuerySet = Report.objects.filter(sample_id="MDX000001")

        # simply qs.get() it since we know that there is only 1 record, otherwise should qs.all()
        # and iterate all matched records for query `sample_id` condition
        r: Report = qs.get()

        logger.info(r)
        logger.info(r.data)
        logger.info(type(r.data))

        # iterate report's data points and show `CN` value
        for d in r.data:
            logger.info(d.get('CN'))

    def test_delete_report(self):
        """
        python manage.py test data_processors.reports.tests.test_report_model.ReportModelUnitTests.test_delete_report
        """
        mock_report: Report = factories.HRDetectReportFactory()  # create mock report through factory fixture
        linked_s3_object: S3Object = factories.ReportLinkedS3ObjectFactory()
        mock_report.s3_object_id = linked_s3_object.id
        mock_report.save()

        qs: QuerySet = Report.objects.filter(id__exact=mock_report.id)  # query it back from db
        self.assertTrue(qs.exists())  # make sure it exists

        report_from_db: Report = qs.get()  # construct back into object
        report_from_db.delete()  # now delete the report
        self.assertEqual(Report.objects.count(), 0)  # none report should exists anymore

        s3_object_from_db = S3Object.objects.get(id__exact=linked_s3_object.id)  # linked S3Object should still exist
        self.assertEqual(s3_object_from_db.id, linked_s3_object.id)

    @override_settings(DEBUG=True)
    def test_delete_report_linked_s3_object(self):
        """
        python manage.py test -v 3 data_processors.reports.tests.test_report_model.ReportModelUnitTests.test_delete_report_linked_s3_object
        """
        logging.getLogger('django.db.backends').setLevel(logging.DEBUG)

        mock_report: Report = factories.HRDetectReportFactory()  # create mock report through factory fixture
        linked_s3_object: S3Object = factories.ReportLinkedS3ObjectFactory()
        mock_report.s3_object_id = linked_s3_object.id
        mock_report.save()

        qs: QuerySet = Report.objects.filter(id__exact=mock_report.id)  # query it back from db
        self.assertTrue(qs.exists())  # make sure it exists

        s3_object_from_db = S3Object.objects.get(id__exact=linked_s3_object.id)
        self.assertEqual(linked_s3_object.id, s3_object_from_db.id)

        s3_object_from_db.delete()  # now delete S3Object

        r: Report = qs.get()
        self.assertIsNotNone(r.s3_object_id)
        self.assertEqual(Report.objects.count(), 1)  # report should exist
        self.assertEqual(S3Object.objects.count(), 0)  # linked S3Object should NOT exist


class ReportModelIntegrationTests(ReportIntegrationTestCase):
    pass
