import csv
import io
import logging
from unittest import TestCase, skip

from data_processors import const
from utils import libgdrive, libssm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LibGDriveUnitTests(TestCase):
    pass


class LibGDriveIntegrationTests(TestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_download_sheet1_csv(self):
        """
        python manage.py test utils.tests.test_libgdrive.LibGDriveIntegrationTests.test_download_sheet1_csv
        """
        lims_sheet_id = libssm.get_secret(const.LIMS_SHEET_ID)
        account_info = libssm.get_secret(const.GDRIVE_SERVICE_ACCOUNT)

        bytes_data = libgdrive.download_sheet1_csv(account_info, lims_sheet_id)
        self.assertIsInstance(bytes_data, bytes)

        # try parse and print last few rows
        csv_reader = csv.DictReader(io.TextIOWrapper(io.BytesIO(bytes_data)))
        for row_number, row in enumerate(csv_reader):
            if row_number > 4116:
                print(row_number, row)
