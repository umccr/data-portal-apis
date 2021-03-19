import logging
from datetime import datetime

from django.db.models import QuerySet
from django.test import TestCase
from django.utils.timezone import now, make_aware

from data_portal.models import S3Object, Report, ReportType
from data_portal.tests import factories
from data_processors.s3 import lambdas

logger = logging.getLogger()


class ReportsTests(TestCase):

    def setUp(self) -> None:
        super(ReportsTests, self).setUp()

    def test_sqs_s3_event_processor(self):
        """
        python manage.py test data_processors.reports.tests.ReportsTests.test_sqs_s3_event_processor

        Test whether the report can be consumed from the SQS queue as expected
        """
        # jq . SBJ00670__SBJ00670_MDX210005_L2100047_rerun-hrdetect.json
        # [
        #   {
        #     "sample": "SBJ00670_MDX210005_L2100047_rerun",
        #     "Probability": 0.034,
        #     "intercept": -3.364,
        #     "del.mh.prop": -0.757,
        #     "SNV3": 2.571,
        #     "SV3": -0.877,
        #     "SV5": -1.105,
        #     "hrdloh_index": 0.096,
        #     "SNV8": 0.079
        #   }
        # ]
        # Compose test data

        bucket_name = 'some-bucket'
        report_to_deserialize = 'cancer_report_tables/json/hrd/SBJ00670__SBJ00670_MDX210005_L2100047_rerun-hrdetect.json.gz'

        # Create an S3Object first with the key to be deleted
        s3_object = S3Object(bucket=bucket_name, key=report_to_deserialize,
                             size=0, last_modified_date=now(), e_tag='')
        s3_object.save()

        s3_event_message = {
            "Records": [
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectRemoved",
                    "s3": {
                        "bucket": {
                            "name": bucket_name,
                        },
                        "object": {
                            "key": report_to_deserialize,
                            "size": 1,
                            "eTag": "object eTag",
                        }
                    }
                },
                {
                    "eventTime": "2019-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated",
                    "s3": {
                        "bucket": {
                            "name": bucket_name
                        },
                        "object": {
                            "key": report_to_deserialize,
                            "size": 1,
                            "eTag": "object eTag",
                        }
                    }
                }
            ]
        }

        sqs_event = {
            "Records": [
                {
                    "body": str(s3_event_message),
                }
            ]
        }

        results = lambdas.handler(sqs_event, None)
        logger.info("MOOOOOOOO")
        logger.info(results)
        self.assertIsNotNone(results)

    def test_umccrise_report_model(self):
        """
        python manage.py test data_processors.reports.tests.ReportsTests.test_umccrise_report_model
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
        """
        python manage.py test data_processors.reports.tests.ReportsTests.test_sv_melted_report_filter
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
        python manage.py test data_processors.reports.tests.ReportsTests.test_create_report
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
            type=ReportType.PURPLE_CNV_SOM.name,
            date_created=make_aware(datetime.now()),
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
