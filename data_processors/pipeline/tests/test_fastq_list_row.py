from unittest import skip

from libiap.openapi import libgds
from libica.openapi import libwes
from mockito import when
import json

from data_processors.pipeline.lambdas import fastq_list_row

from data_processors.pipeline.lambdas import orchestrator
from data_processors.pipeline.tests import _rand
from data_processors.pipeline import services
from data_portal.models import SequenceRun
from data_portal.tests.factories import SequenceRunFactory
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class FastqListRowUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(FastqListRowUnitTests, self).setUp()

        # Initialise mock wes run
        mock_bcl_convert_wes_run: libwes.WorkflowRun = libwes.WorkflowRun()

        mock_bcl_convert_wes_run.output = json.dumps({
            "main/fastq_list_rows":
                [
                    {"lane": 4,
                     "read_1":
                         {"basename": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz",
                          "class": "File",
                          "http://commonwl.org/cwltool#generation": 0,
                          "location": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz",
                          "nameext": ".gz",
                          "nameroot": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq",
                          "size": 26376114564},
                     "read_2":
                         {"basename": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq.gz",
                          "class": "File",
                          "http://commonwl.org/cwltool#generation": 0,
                          "location": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq.gz",
                          "nameext": ".gz",
                          "nameroot": "PTC_TSOctDNA200901VD_L2000753_S1_L004_R2_001.fastq",
                          "size": 25995547898},
                     "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                     "rgsm": "PTC_TSOctDNA200901VD_L2000753",
                     "rglb": "UnknownLibrary"
                     },
                    {
                        "lane": 3,
                        "read_1":
                            {"basename": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq.gz",
                             "class": "File",
                             "http://commonwl.org/cwltool#generation": 0,
                             "location": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq.gz",
                             "nameext": ".gz",
                             "nameroot": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R1_001.fastq",
                             "size": 26376114564},
                        "read_2":
                            {"basename": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq.gz",
                             "class": "File",
                             "http://commonwl.org/cwltool#generation": 0,
                             "location": "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq.gz",
                             "nameext": ".gz",
                             "nameroot": "PTC_TSOctDNA200901VD_L2000753_topup_S1_L004_R2_001.fastq",
                             "size": 25995547898},
                        "rgid": "CCGTGACCGA.CCGAACGTTG.4",
                        "rgsm": "PTC_TSOctDNA200901VD_L2000753_topup",
                        "rglb": "UnknownLibrary"
                    }
                ]
        })

        when(libwes).WorkflowRun(...).thenReturn(mock_bcl_convert_wes_run)

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_fastq_list_row.FastqListRowUnitTests.test_handler

        Parse a standard bclconvert output fastq_list_rows objects to be ready to be imported as a fastq list obj
        """

        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_bcl_convert_wes_run: libwes.WorkflowRun = libwes.WorkflowRun()

        # Call fastq handler on mock_fastq_list_rows
        mock_fastq_list_handler = fastq_list_row.handler({'fastq_list_rows': orchestrator.parse_bcl_convert_output(mock_bcl_convert_wes_run.output),
                                                          'sequence_run_id': mock_sqr.run_id}, None)

        # Assertion 1: Ensure output is of type list
        self.assertTrue(type(mock_fastq_list_handler) == list)

        # Assertion 2: Ensure list is of length 1
        self.assertTrue(len(mock_fastq_list_handler) == 2)

        # Assertion 3: Ensure that element 0 of list is of type dict
        self.assertTrue(type(mock_fastq_list_handler[0]) == dict)

        # Assertion 4: Ensure that the rgsm value equals <name_of_sample>
        self.assertTrue(mock_fastq_list_handler[0]["rgsm"] == "PTC_TSOctDNA200901VD")

        # Assertion 5: Ensure that the rglb value equals <name_of_library>
        self.assertTrue(mock_fastq_list_handler[0]["rglb"] == "L2000753")

        # Assertion 6: Ensure that the read_1 value equals <gds_location_foo>
        self.assertTrue(mock_fastq_list_handler[0]["read_1"] == "gds://wfr.3157562b798b44009f661549aa421815/bcl-convert-test/steps/bcl_convert_step/0/steps/bclConvert-nonFPGA-3/try-1/U7N1Y93N50_I10_I10_U7N1Y93N50/PTC_TSOctDNA200901VD_L2000753_S1_L004_R1_001.fastq.gz")

        # Assertion 7: Ensure that _topup is removed from rglb
        self.assertTrue(mock_fastq_list_handler[1]["rglb"] == "L2000753")

        # Assertion 8: Ensure that rgid still has _topup attribute
        self.assertTrue(mock_fastq_list_handler[1]["rgid"].endswith("_topup"))


    def add_fastq_list_row_as_fastq_list_object(self):
        """
        python manage.py test data_processors.pipeline.tests.test_fastq_list_row.FastqListRowUnitTests.add_fastq_list_row_as_fastq_list_object
        :return:
        """

        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_bcl_convert_wes_run: libwes.WorkflowRun = libwes.WorkflowRun()

        # Call fastq handler on mock_fastq_list_rows
        mock_fastq_list_handler = fastq_list_row.handler({'fastq_list_rows': orchestrator.parse_bcl_convert_output(mock_bcl_convert_wes_run.output),
                                                          'sequence_run_id': mock_sqr.run_id}, None)

        services.create_fastq_list_row(mock_fastq_list_handler[0],
                                       sequence_run=mock_bcl_convert_wes_run.run_id)