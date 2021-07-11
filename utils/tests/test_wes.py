from unittest import skip

from libica.openapi import libwes

from utils import wes
from utils.tests.test_ica import IcaUnitTests, IcaIntegrationTests, logger


class WesUnitTests(IcaUnitTests):

    def test_get_run(self):
        """
        python manage.py test utils.tests.test_wes.WesUnitTests.test_get_run
        """
        result = wes.get_run("wfr.13245678903214565")
        self.assertTrue(isinstance(result, libwes.WorkflowRun))
        self.assertIsNotNone(result)
        logger.info(f"EXAMPLE MOCK WorkflowRun: \n{result}")

    def test_get_run_to_dict(self):
        """
        python manage.py test utils.tests.test_wes.WesUnitTests.test_get_run_to_dict
        """
        result = wes.get_run("wfr.13245678903214565", to_dict=True)
        self.assertTrue(isinstance(result, dict))
        self.assertIsNotNone(result)
        logger.info(f"EXAMPLE MOCK WorkflowRun to_dict(): \n{result}")

    def test_get_run_to_json(self):
        """
        python manage.py test utils.tests.test_wes.WesUnitTests.test_get_run_to_json
        """
        result = wes.get_run("wfr.13245678903214565", to_json=True)
        self.assertTrue(isinstance(result, str))
        self.assertIsNotNone(result)
        logger.info(f"EXAMPLE MOCK WorkflowRun to_json(): \n{result}")


class WesIntegrationTests(IcaIntegrationTests):

    @skip
    def test_get_run(self):
        """
        python manage.py test utils.tests.test_wes.WesIntegrationTests.test_get_run
        """
        wfr_id = "wfr.81cf25d7226a4874be43e4b15c1f5687"
        result = wes.get_run(wfr_id, to_dict=True)
        self.assertEqual(result['id'], wfr_id)
        self.assertIsNotNone(result['input'])
        self.assertIsNotNone(result['output'])

        # You may peak the result like so
        # print(result['input'])
        # print(result['output'])
