from django.test import TestCase

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.viewsets.tests import _logger


class FastqListRowViewSetTestCase(TestCase):

    def setUp(self):
        FastqListRow.objects.create(
            id=1,
            rgid="AGTCCTCC.2.200612_A01052_0017_ABCDEFGHIJ.ABC_SSSSSS200323LL_L2000006",
            rgsm="ABC_SSSSSS200323LL",
            rglb="L2000006",
            lane=2,
            read_1="gds://some path",
            read_2="gds://some path"
        )

    def test_get_api(self):
        """
        python manage.py test data_portal.viewsets.tests.test_fastqlistrow.FastqListRowViewSetTestCase.test_get_api
        """
        # Get metadata list
        _logger.info('Get fastq API')
        response = self.client.get('/fastq/')
        self.assertEqual(response.status_code, 200, 'Ok status response is expected')

        _logger.info('Check if API return result')
        result_response = response.data['results']
        self.assertGreater(len(result_response), 0, 'A result is expected')

        _logger.info('Check if unique data has a single entry')
        response = self.client.get('/fastq/?rglb=L2000006')
        results_response = response.data['results']
        self.assertEqual(len(results_response), 1, 'Single result is expected for unique data')
