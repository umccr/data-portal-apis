import json

from mockito import when

from data_portal.models.sequencerun import SequenceRun
from data_portal.tests.factories import SequenceRunFactory
from data_processors.pipeline.lambdas import fastq_list_row
from data_processors.pipeline.services import fastq_srv
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class FastqListRowUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(FastqListRowUnitTests, self).setUp()

        # Struct from bcl convert output > main/fastq_list_rows
        # Refer: Example IAP Run > Outputs
        # https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.84abc203cabd4dc196a6cf9bb49d5f74/3.7.5.md
        self.mock_fastq_list_rows = [
            {
                "lane": 4,
                "read_1":
                    {
                        "basename": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz",
                        "class": "File",
                        "http://commonwl.org/cwltool#generation": 0,
                        "location": "gds://vol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz",
                        "nameext": ".gz",
                        "nameroot": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq",
                        "size": 26376114564
                    },
                "read_2":
                    {
                        "basename": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq.gz",
                        "class": "File",
                        "http://commonwl.org/cwltool#generation": 0,
                        "location": "gds://vol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq.gz",
                        "nameext": ".gz",
                        "nameroot": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq",
                        "size": 25995547898
                    },
                "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                "rgsm": "PTC_TSOctDNA200901VD_L2000753",
                "rglb": "UnknownLibrary"
            },
            {
                "lane": 3,
                "read_1":
                    {
                        "basename": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq.gz",
                        "class": "File",
                        "http://commonwl.org/cwltool#generation": 0,
                        "location": "gds://vol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq.gz",
                        "nameext": ".gz",
                        "nameroot": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq",
                        "size": 26376114564
                    },
                "read_2":
                    {
                        "basename": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq.gz",
                        "class": "File",
                        "http://commonwl.org/cwltool#generation": 0,
                        "location": "gds://vol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq.gz",
                        "nameext": ".gz",
                        "nameroot": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq",
                        "size": 25995547898
                    },
                "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                "rgsm": "PTC_TSOctDNA200901VD_L2000753_topup",
                "rglb": "UnknownLibrary"
            }
        ]

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq_list_row.FastqListRowUnitTests.test_handler

        Parse a standard bcl convert output fastq_list_rows objects to be ready to be imported as a fastq list object
        """
        self.verify_local()

        mock_sqr: SequenceRun = SequenceRunFactory()

        result = fastq_list_row.handler({
            'fastq_list_rows': self.mock_fastq_list_rows,
            'seq_name': mock_sqr.name,
        }, None)

        logger.info("-" * 32)
        logger.info("Example fastq_list_row.handler lambda output:")
        logger.info(json.dumps(result))

        # Assertion 1: Ensure output is of type list
        self.assertTrue(type(result) == list)

        # Assertion 2: Ensure list is of length 1
        self.assertTrue(len(result) == 2)

        # Assertion 3: Ensure that element 0 of list is of type dict
        self.assertTrue(type(result[0]) == dict)

        # Assertion 4: Ensure that the rgsm value equals <name_of_sample>
        self.assertTrue(result[0]['rgsm'] == "PTC_TSOctDNA200901VD")

        # Assertion 5: Ensure that the rglb value equals <name_of_library>
        self.assertTrue(result[0]['rglb'] == "L2000753")

        # Assertion 6: Ensure that the read_1 value equals <gds_location_foo>
        self.assertTrue(result[0]['read_1'] == "gds://vol/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz")

        # Assertion 7: Ensure that _topup is removed from rglb
        self.assertTrue(result[1]['rglb'] == "L2000753")

        # Assertion 8: Ensure that rgid still has _topup attribute
        self.assertTrue(result[1]['rgid'].endswith("_topup"))

    def test_handler_file_not_found(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq_list_row.FastqListRowUnitTests.test_handler_file_not_found
        """
        read_1_location = f"gds://{self.mock_fastq_list_rows[0]['read_1']['location']}"
        when(fastq_list_row.gds).check_file(...).thenRaise(FileNotFoundError(f"Could not get file: {read_1_location}"))

        try:
            fastq_list_row.handler({
                'fastq_list_rows': self.mock_fastq_list_rows,
                'seq_name': "does-not-matter",
            }, None)
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(FileNotFoundError)

    def test_create_fastq_list_row(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_fastq_list_row.FastqListRowUnitTests.test_create_fastq_list_row
        """
        self.verify_local()

        mock_sqr: SequenceRun = SequenceRunFactory()

        # Call fastq handler on mock_fastq_list_rows
        result = fastq_list_row.handler({
            'fastq_list_rows': self.mock_fastq_list_rows,
            'seq_name': mock_sqr.name
        }, None)

        stub_flr = result[0]

        # Create row
        fastq_srv.create_or_update_fastq_list_row(stub_flr, sequence_run=mock_sqr)

        # Get row
        flr_in_db = fastq_srv.get_fastq_list_row_by_rgid(rgid=stub_flr['rgid'])

        logger.info("-" * 32)
        logger.info("Example FastqListRow from db:")
        logger.info(flr_in_db)

        # assert row is in database
        self.assertTrue(flr_in_db.rgsm == stub_flr['rgsm'])
