import json
import tempfile
import warnings
from unittest import skip

import pandas as pd
import requests
from libiap.openapi import libgds
from mockito import when, mock

from data_portal.models import SequenceRun
from data_portal.tests.factories import SequenceRunFactory
from data_processors.pipeline.lambdas import demux_metadata
from data_processors.pipeline.tests.case import logger, PipelineUnitTestCase, PipelineIntegrationTestCase


class DemuxMetaDataTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(DemuxMetaDataTests, self).setUp()
        self.mock_sample_sheet = tempfile.NamedTemporaryFile()
        self.mock_sample_sheet_content = b"""
        [Header],,,,,,,,,,,
        IEMFileVersion,5,,,,,,,,,,
        Experiment Name,Mock-EXP200908_11Sept20,,,,,,,,,,
        Date,11/09/2020,,,,,,,,,,
        Workflow,GenerateFASTQ,,,,,,,,,,
        Application,NovaSeq FASTQ Only,,,,,,,,,,
        Instrument Type,NovaSeq,,,,,,,,,,
        Assay,TruSeq Nano DNA,,,,,,,,,,
        Index Adapters,IDT-ILMN TruSeq DNA UD Indexes (96 Indexes),,,,,,,,,,
        Chemistry,Amplicon,,,,,,,,,,
        ,,,,,,,,,,,
        [Reads],,,,,,,,,,,
        100,,,,,,,,,,,
        100,,,,,,,,,,,
        ,,,,,,,,,,,
        [Settings],,,,,,,,,,,
        Adapter,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA,,,,,,,,,,
        AdapterRead2,GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG,,,,,,,,,,
        ,,,,,,,,,,,
        [Data],,,,,,,,,,,
        Lane,Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description
        1,PTC_EXPn200908LL_L2000001,,,,,,,,,,
        """
        self.mock_sample_sheet.write(self.mock_sample_sheet_content)
        self.mock_sample_sheet.seek(0)
        self.mock_sample_sheet.read()

        d = {
            demux_metadata.SAMPLE_ID_HEADER: [
                "PTC_EXPn200908LL_L2000001",
            ],
            demux_metadata.OVERRIDECYCLES_HEADER: [
                "Y100;I8N2;I8N2;Y100",
            ],
            demux_metadata.TYPE_HEADER: [
                "WGS",
            ],
            demux_metadata.ASSAY_HEADER: [
                "TsqNano"
            ]
        }
        self.mock_metadata_df = pd.DataFrame(data=d)

    def tearDown(self) -> None:
        self.mock_sample_sheet.close()
        super(DemuxMetaDataTests, self).tearDown()

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux_metadata.DemuxMetaDataTests.test_handler
        """
        # See https://github.com/clintval/sample-sheet/blob/master/sample_sheet/__init__.py#L567
        warnings.simplefilter("ignore")

        mock_sqr: SequenceRun = SequenceRunFactory()

        mock_metadata_event = {
            'gdsVolume': mock_sqr.gds_volume_name,
            'gdsBasePath': mock_sqr.gds_folder_path,
            'gdsSamplesheet': "SampleSheet.csv",
        }

        when(demux_metadata).download_gds_file(...).thenReturn(self.mock_sample_sheet.name)
        when(demux_metadata).download_metadata(...).thenReturn(self.mock_metadata_df)  # comment to fetch actual sheet

        # Get records list
        result = demux_metadata.handler(mock_metadata_event, None)

        # Make sure that the output is a list
        self.assertIsInstance(result, list)

        # Make sure that a selected attribute has 'type', 'sample', 'override-cycles', and 'assay'
        self.assertIsNotNone(result[0].get('sample', None))
        self.assertIsNotNone(result[0].get('override_cycles', None))
        self.assertIsNotNone(result[0].get('type', None))
        self.assertIsNotNone(result[0].get('assay', None))

        logger.info("-" * 32)
        logger.info("Example demux_metadata.handler lambda output:")
        logger.info(json.dumps(result))

    def test_download_gds_file(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux_metadata.DemuxMetaDataTests.test_download_gds_file
        """

        mock_file_list: libgds.FileListResponse = libgds.FileListResponse()
        mock_file_list.items = [
            libgds.FileResponse(name="SampleSheet.csv"),
        ]
        when(libgds.FilesApi).list_files(...).thenReturn(mock_file_list)

        mock_file = libgds.FileResponse()
        mock_file.id = "fil.333888aaaafe44ab1f7f08d8505c19ab"
        mock_file.name = "SampleSheet.csv"
        mock_file.path = "/Runs/200911_A00130_0001_AAAAAAAAAA_r.v0_aiefjIWflxIkmv/SampleSheet.csv"
        mock_file.presigned_url = "https://mock.s3.ap-southeast-2.amazonaws.com/888abc888-d44a-4343-85b3-88d77f441232" \
                                  "/Runs/200911_A00130_0001_AAAAAAAAAA_r.v0_aiefjIWflxIkmv/SampleSheet.csv?" \
                                  "X-Amz-Expires=604800" \
                                  "&response-content-disposition=attachment%3Bfilename%3D%22SampleSheet.csv%22" \
                                  "&response-content-type=application%2Foctet-stream" \
                                  "&x-userId=44ed4444-777c-888e-be44-cf9ae7373f73" \
                                  "&X-Amz-Algorithm=AWS4-HMAC-SHA256" \
                                  "&X-Amz-Credential=AAAAAAAAAAAAAAAAAAAA/20200924/ap-southeast-2/s3/aws4_request" \
                                  "&X-Amz-Date=20200924T041330Z" \
                                  "&X-Amz-SignedHeaders=host" \
                                  "&X-Amz-Signature=0a0c4p44paaab986400ce883cbc3449e7a61a9f93e94e63a33bcf8903a7773bb"
        when(libgds.FilesApi).get_file(...).thenReturn(mock_file)

        mock_resp = mock({'status_code': 200, 'content': self.mock_sample_sheet_content})
        when(requests).get(...).thenReturn(mock_resp)

        local_path = demux_metadata.download_gds_file(gds_volume="you-cannot", gds_path="/find/me/SampleSheet.csv")
        self.assertIsNotNone(local_path)
        logger.info(json.dumps({'local_path': local_path}))

    def test_download_gds_file_not_found(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux_metadata.DemuxMetaDataTests.test_download_gds_file_not_found
        """
        local_path = demux_metadata.download_gds_file(gds_volume="you-cannot", gds_path="/find/me/SampleSheet.csv")
        self.assertIsNone(local_path)


class DemuxMetaDataIntegrationTests(PipelineIntegrationTestCase):

    @skip
    def test_download_metadata(self):
        """
        python manage.py test data_processors.pipeline.tests.test_demux_metadata.DemuxMetaDataIntegrationTests.test_download_metadata
        """
        my_df: pd.DataFrame = demux_metadata.download_metadata("2021")
        print(my_df)
        self.assertIsNotNone(my_df)
        self.assertTrue(not my_df.empty)

    @skip
    def test_handler(self):
        """
        aws sso login --profile dev && export AWS_PROFILE=dev
        python manage.py test data_processors.pipeline.tests.test_demux_metadata.DemuxMetaDataIntegrationTests.test_handler
        """
        result: pd.DataFrame = demux_metadata.handler({
            'gdsVolume': "umccr-raw-sequence-data-dev",
            'gdsBasePath': "/200612_A01052_0017_BH5LYWDSXY",
            'gdsSamplesheet': "SampleSheet.csv",
        }, None)

        print(json.dumps(result))
        self.assertIsNotNone(result)
