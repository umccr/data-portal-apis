from django.test import TestCase

from data_portal.models.libraryrun import LibraryRun
from data_portal.models.workflow import Workflow
from data_portal.tests.factories import DragenWgsQcWorkflowFactory, TestConstant
from data_portal.viewsets.tests import _logger


class LibraryRunViewSetTestCase(TestCase):

    def setUp(self):
        self.mock_qc_wfl: Workflow = DragenWgsQcWorkflowFactory()

        self.mock_lib_run = LibraryRun.objects.create(
            library_id=TestConstant.library_id_tumor.value,
            instrument_run_id=TestConstant.instrument_run_id.value,
            run_id=TestConstant.run_id.value,
            lane=2,
            override_cycles="",
            coverage_yield="",
            qc_pass=True,
            qc_status="poor",
            valid_for_analysis=True,
        )

        self.mock_lib_run.workflows.add(self.mock_qc_wfl)

    def test_get_api(self):
        """
        python manage.py test data_portal.viewsets.tests.test_libraryrun.LibraryRunViewSetTestCase.test_get_api
        """
        # Get sequence list
        _logger.info('Get sequence API')
        response = self.client.get('/libraryrun/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        _logger.info('Check if API return result')
        results_response = response.data['results']
        self.assertGreater(len(results_response), 0, 'At least some result is expected')

        _logger.info('Check if unique data has a single entry')
        q = f'/libraryrun/?instrument_run_id={TestConstant.instrument_run_id.value}&library_id={TestConstant.library_id_tumor.value}'
        response = self.client.get(q)
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for uniqueness')

        _logger.info('Check if wrong parameter')
        response = self.client.get('/libraryrun/?lib_id=LBR0001')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 0, 'No results are expected for unrecognized query parameter')

        _logger.info('Check if related model field as request parameter')
        response = self.client.get(f'/libraryrun/?type_name={self.mock_qc_wfl.type_name}')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Results are expected for query parameter by related model field')

        _logger.info('Check if built-in REST request parameter')
        q = f'/libraryrun/?search={TestConstant.library_id_tumor.value}&rowsPerPage=1000&page=1&ordering=-library_id'
        response = self.client.get(q)
        results_response = response.data['results']
        _logger.info(response.data)
        self.assertEqual(len(results_response), 1, 'Results are expected for built-in REST query parameter')
