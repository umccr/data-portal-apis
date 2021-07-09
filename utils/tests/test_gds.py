import csv
import json
import os
from contextlib import closing
from tempfile import NamedTemporaryFile
from typing import List
from unittest import TestCase, skip

import requests
from libica.openapi import libgds
from mockito import when, mock

from utils import gds
from utils.tests.test_ica import IcaUnitTests, logger

gds_path = "gds://umccr-fastq-data-dev/200612_A01052_0017_BH5LYWDSXY/TSO-DNA_TSODNA/UMCCR/PTC_TSO200609VD_L2000552_S1_L003_R1_001.fastq.gz"


class GdsUnitTests(IcaUnitTests):

    def test_parse_gds_path(self):
        """
        python manage.py test utils.tests.test_gds.GdsUnitTests.test_parse_gds_path
        """
        vol, path_ = gds.parse_path(gds_path)
        logger.info(vol)
        logger.info(path_)
        self.assertEqual(vol, "umccr-fastq-data-dev")
        self.assertTrue(path_.startswith("/"))

    def test_get_gds_file_list(self):
        """
        python manage.py test utils.tests.test_gds.GdsUnitTests.test_get_gds_file_list
        """
        self.verify_local()
        vol, path_ = gds.parse_path(gds_path)
        files: List[libgds.FileResponse] = gds.get_file_list(volume_name=vol, path=path_)
        self.assertIsNotNone(files)
        if len(files) > 1:
            logger.info(f"File info: \n{files[0]}")  # grab first one and show its struct

    def test_check_path(self):
        """
        python manage.py test utils.tests.test_gds.GdsUnitTests.test_check_path
        """
        self.verify_local()

        mock_flr = libgds.FileListResponse()
        mock_flr.items = [
            libgds.FileResponse(
                volume_name="VOL",
                path="/I_AM_HERE.csv"
            )
        ]
        when(gds.libgds.FilesApi).list_files(...).thenReturn(mock_flr)

        files: List[libgds.FileResponse] = gds.check_path("VOL", "/I_AM_HERE.csv")
        file: libgds.FileResponse = files[0]
        logger.info(f"File info: \n{file}")
        self.assertEqual(file.volume_name, "VOL")

    def test_check_file_not_found(self):
        """
        python manage.py test utils.tests.test_gds.GdsUnitTests.test_check_file_not_found
        """
        self.verify_local()

        mock_flr = libgds.FileListResponse()
        mock_flr.items = []
        when(gds.libgds.FilesApi).list_files(...).thenReturn(mock_flr)

        try:
            gds.check_file("gds://YOU_KNOW/I_AM_NOT_EXIST.csv")
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(FileNotFoundError)

    def test_download_gds_file(self):
        """
        python manage.py test utils.tests.test_gds.GdsUnitTests.test_download_gds_file
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

        mock_resp = mock({'status_code': 200, 'content': b"mock"})
        when(requests).get(...).thenReturn(mock_resp)

        ntf: NamedTemporaryFile() = gds.download_gds_file("you-cannot", "/find/me/SampleSheet.csv")
        self.assertIsNotNone(ntf)
        logger.info(json.dumps({'local_path': ntf.name}))
        with closing(ntf) as f:
            with open(f.name) as file:
                out = file.read()
                logger.info(out)
                self.assertEqual(out, "mock")

    def test_download_gds_file_not_found(self):
        """
        python manage.py test utils.tests.test_gds.GdsUnitTests.test_download_gds_file_not_found
        """
        self.verify_local()
        local_path = gds.download_gds_file("you-cannot", "/find/me/SampleSheet.csv")
        self.assertIsNone(local_path)


class GdsIntegrationTests(TestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    def setUp(self) -> None:
        self.vol, self.path = gds.parse_path(gds_path)

    @skip
    def test_get_gds_file_list(self):
        """
        python manage.py test utils.tests.test_gds.GdsIntegrationTests.test_get_gds_file_list
        """
        files: List[libgds.FileResponse] = gds.get_file_list(volume_name=self.vol, path=self.path)
        logger.info(f"List of files: \n{files}")
        self.assertEqual(files[0].path, self.path)

    @skip
    def test_check_file(self):
        """
        python manage.py test utils.tests.test_gds.GdsIntegrationTests.test_check_file
        """
        files: List[libgds.FileResponse] = gds.check_file(gds_path)
        file: libgds.FileResponse = files[0]
        logger.info(f"File info: \n{file}")
        self.assertEqual(f"gds://{file.volume_name}{file.path}", gds_path)

    @skip
    def test_check_file_not_found(self):
        """
        python manage.py test utils.tests.test_gds.GdsIntegrationTests.test_check_file_not_found
        """
        try:
            gds.check_file("gds://umccr-fastq-data-dev/I_AM_NOT_EXIST.csv")
        except Exception as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(FileNotFoundError)

    @skip
    def test_download_gds_file(self):
        """
        python manage.py test utils.tests.test_gds.GdsIntegrationTests.test_download_gds_file
        """
        dl_path = "gds://umccr-fastq-data-dev/200612_A01052_0017_BH5LYWDSXY/SampleSheet.TSO-DNA_TSODNA.csv"

        vol, path = gds.parse_path(dl_path)
        ntf: NamedTemporaryFile = gds.download_gds_file(vol, path)
        self.assertIsNotNone(ntf)

        tmp_path = ntf.name
        logger.info(f"NamedTemporaryFile.name: {tmp_path}")

        with closing(ntf) as f:
            with open(f.name, newline='') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        print(f"Column names: {', '.join(row)}")
                        line_count += 1
                    else:
                        line_count += 1
                print(f'Processed {line_count} lines.')
                self.assertTrue(line_count > 0)

        # ntf.close()  # Or close like this
        self.assertFalse(os.path.exists(tmp_path))
