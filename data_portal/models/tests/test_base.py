import logging

from django.db.models import Q, QuerySet
from django.test import TestCase
from django.utils.timezone import now

from data_portal.models.base import PortalBaseManager, PortalBaseModel
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.limsrow import LIMSRow
from data_portal.tests.factories import TestConstant

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PortalBaseManagerTestCase(TestCase):

    def setUp(self) -> None:
        LIMSRow.objects.create(
            illumina_id=TestConstant.instrument_run_id.value,
            run=1,
            timestamp=now().date(),
            subject_id=TestConstant.subject_id.value,
            sample_id=TestConstant.sample_id.value,
            library_id=TestConstant.library_id_tumor.value,
        )

    def test_reduce_multi_values_qor(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_reduce_multi_values_qor
        """
        q = PortalBaseManager.reduce_multi_values_qor('subject_id', ["SBJ000001", "SBJ000002"])
        logger.info(q)
        self.assertIsNotNone(q)
        self.assertIsInstance(q, Q)
        self.assertIn(Q.OR, str(q))

    def test_reduce_multi_values_qor_auto_pack(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_reduce_multi_values_qor_auto_pack
        """
        q = PortalBaseManager.reduce_multi_values_qor('subject_id', "SBJ000001")
        logger.info(q)
        self.assertIsNotNone(q)
        self.assertIsInstance(q, Q)
        self.assertIn(Q.AND, str(q))

    def test_get_model_fields_query(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_get_model_fields_query
        """
        qs: QuerySet = LIMSRow.objects.get_queryset()
        self.assertEqual(1, qs.count())
        rqs = LIMSRow.objects.get_model_fields_query(qs, **{'subject_id': [TestConstant.subject_id.value]})
        logger.info(f"\n{rqs.query}")
        self.assertIsNotNone(rqs)
        self.assertIn(TestConstant.subject_id.value, str(rqs.query))
        self.assertEqual(1, rqs.count())

    def test_get_model_fields_query_invalid_param(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_get_model_fields_query_invalid_param
        """
        qs: QuerySet = LIMSRow.objects.get_queryset()
        self.assertEqual(1, qs.count())
        rqs = LIMSRow.objects.get_model_fields_query(qs, **{'this_return_empty': [TestConstant.subject_id.value]})
        logger.info(f"\n{rqs}")
        self.assertEqual(0, rqs.count())

    def test_get_model_fields_query_search_param(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_get_model_fields_query_search_param
        """
        rqs = LIMSRow.objects.get_by_keyword(**{
            'search': [TestConstant.subject_id.value],
            'ordering': "-subject_id",
            'rowsPerPage': 1000,
            'sortCol': "subject_id",
            'sortAsc': True,
        })
        logger.info(rqs)
        logger.info(f"\n{rqs.query}")
        self.assertEqual(1, rqs.count())

    def test_portal_base_model_must_abstract(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_portal_base_model_must_abstract
        """
        try:
            PortalBaseModel()
        except TypeError as e:
            logger.exception(f"THIS ERROR EXCEPTION IS INTENTIONAL FOR TEST. NOT ACTUAL ERROR. \n{e}")
        self.assertRaises(TypeError)

    def test_portal_base_model_get_base_fields(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_portal_base_model_get_base_fields
        """
        fields = LibraryRun.get_base_fields()
        logger.info(fields)
        self.assertIn('library_id', fields)
        self.assertNotIn('workflows', fields)

    def test_portal_base_model_get_fields(self):
        """
        python manage.py test data_portal.models.tests.test_base.PortalBaseManagerTestCase.test_portal_base_model_get_fields
        """
        fields = LibraryRun.get_fields()
        logger.info(fields)
        self.assertIn('library_id', fields)
        self.assertIn('workflows', fields)
