from unittest.mock import MagicMock, Mock

from django.db.models import Q
from django.db.models.query import QuerySet
from django.test import TestCase

from data_portal.exceptions import InvalidSearchQuery
from data_portal.search_query import SearchQueryHelper, FilterMethodFactory, FilterTagFactory, FilterTypeFactory, \
    FilterTag, FilterType, FilterMethod


class SearchQueryTests(TestCase):
    """
    Test cases for (base) search query processing functionality.
    If classes/methods in `search_query` has been extended/overridden, we should have a separate test file
    testing the more specific/integrated functionality (e.g `test_s3_object_search`).
    """
    def setUp(self) -> None:
        # Create mocked queryset so that we can record what has been passed to filter() method
        self.queryset = QuerySet()
        self.queryset.filter = Mock(side_effect=lambda args: self._record_filter_arguments(self.queryset, args))
        self.queryset.distinct = Mock()
        self.exposed_filter_q = Q()

        # Set up some sample filter methods, tags, types and factories for testing
        self.filter_method_factory = FilterMethodFactory()
        self.filter_tag_factory = FilterTagFactory()
        self.filter_type_factory = FilterTypeFactory(
            filter_tag_factory=self.filter_tag_factory,
            filter_method_factory=self.filter_method_factory
        )

        filter_tag_string_name = "TAG_STRING"
        string_field_names = ("string_field_1", )
        self.filter_tag_factory.register(FilterTag(
            name=filter_tag_string_name,
            value_parser=str,
            field_names=string_field_names)
        )

        filter_tag_int_name = "TAG_INT"
        string_field_names = ("integer_field_1", )
        self.filter_tag_factory.register(FilterTag(
            name=filter_tag_int_name,
            value_parser=int,
            field_names=string_field_names)
        )

        self.type_name_compare = "int_compare"
        self.filter_type_factory.register(FilterType(
            name=self.type_name_compare,
            tag=self.filter_tag_factory.get(filter_tag_int_name),
            method=self.filter_method_factory.get(FilterMethod.COMPARE),
            description="test description"
        ))

        self.type_name_contains = "string_contains"
        self.filter_type_factory.register(FilterType(
            name=self.type_name_contains,
            tag=self.filter_tag_factory.get(filter_tag_string_name),
            method=self.filter_method_factory.get(FilterMethod.CONTAINS),
            description="test description"
        ))

        self.type_name_ends_with = "string_ends_with"
        self.filter_type_factory.register(FilterType(
            name=self.type_name_ends_with,
            tag=self.filter_tag_factory.get(filter_tag_string_name),
            method=self.filter_method_factory.get(FilterMethod.ENDS_WITH),
            description="test description"
        ))

        # Also set a default type for testing
        self.filter_type_factory.set_default(self.type_name_contains)

        self.helper = SearchQueryHelper(
            filter_type_factory=self.filter_type_factory,
            filter_method_factory=self.filter_method_factory,
            filter_tag_factory=self.filter_tag_factory
        )

    def tearDown(self) -> None:
        # Reset exposed filter query
        self.exposed_filter_q = Q()

    def _record_filter_arguments(self, queryset, filter_arguments) -> None:
        """
        Side effect method used for queryset object, recording the arguments passed into queryset.filter()
        """
        self.exposed_filter_q = self.exposed_filter_q & filter_arguments
        return queryset

    def _set_parsed_filters(self, filters) -> None:
        """
        Mock the `helper._parse_raw_query` method so that we can directly set parsed filters for testing
        """
        self.helper._parse_raw_query = MagicMock(return_value=filters)

    def test_parse_raw_query_empty(self):
        """
        Test we can accept an empty query string
        """
        filters = self.helper._parse_raw_query("")
        # No filter should have been applied
        self.assertEqual(len(filters), 0)

    def test_parse_raw_query_default(self):
        """
        Test query string without : will be treated as the correct default filter type
        """
        filters = self.helper._parse_raw_query("some_value")
        self.assertTrue(self.filter_type_factory.get_default().name in filters)

    def test_parse_raw_query_contains(self):
        """
        Test we can parse raw query with valid string 'contains' filter
        """
        val = "some_string"
        filters = self.helper._parse_raw_query(f'{self.type_name_contains}:{val}')

        # Check at high level we have the right number of filters
        self.assertTrue(self.type_name_contains in filters)
        contains_filter = filters[self.type_name_contains][0]

        # We have parsed in the right filter type and value.
        self.assertEqual(contains_filter.filter_type, self.filter_type_factory.get(self.type_name_contains))
        self.assertEqual(contains_filter.val, val)

        # Now we test that whether the queryset can be updated correctly
        self._set_parsed_filters(filters)
        self.helper.parse("", self.queryset)
        filter_type = self.filter_type_factory.get(self.type_name_contains)
        expected_q = Q()

        # For this case we only test with one field name
        q_arg = f'{filter_type.tag.field_names[0]}__icontains'
        expected_q = expected_q | Q(**{q_arg: val})

        self.assertEqual(self.exposed_filter_q, expected_q)

    def test_parse_raw_query_ends_with(self):
        """
        Test we can parse raw query with valid string 'ends with' filter
        """
        val = "some_string"
        filters = self.helper._parse_raw_query(f'{self.type_name_ends_with}:{val}')

        # Check at high level we have the right number of filters
        self.assertTrue(self.type_name_ends_with in filters)
        contains_filter = filters[self.type_name_ends_with][0]

        # We have parsed in the right filter type and value.
        self.assertEqual(contains_filter.filter_type, self.filter_type_factory.get(self.type_name_ends_with))
        self.assertTrue(contains_filter.val, val)

        # Now we test that whether the queryset can be updated correctly
        self._set_parsed_filters(filters)
        self.helper.parse("", self.queryset)
        filter_type = self.filter_type_factory.get(self.type_name_ends_with)
        expected_q = Q()

        # For this case we only test with one field name
        q_arg = f'{filter_type.tag.field_names[0]}__iendswith'
        expected_q = expected_q | Q(**{q_arg: val})

        self.assertEqual(self.exposed_filter_q, expected_q)

    def test_parse_raw_query_comparison(self):
        """
        Test we can parse raw query with valid number/date 'comparison' filter
        """
        # Make each comparator having a different value so we can better check the parsing logic
        comparator_val_dict = {"<": "1", "<=": "2", ">": "3", ">=": "4", "=": "5"}
        raw_query_tokens = [f'{self.type_name_compare}:{comp}{val}' for comp, val in comparator_val_dict.items()]
        raw_query = "  ".join(raw_query_tokens)

        filters = self.helper._parse_raw_query(query_raw=raw_query)

        # Check at high level we have the right number of filters
        self.assertTrue(self.type_name_compare in filters)
        compare_filters = filters[self.type_name_compare]
        self.assertEqual(len(compare_filters), 5)

        # Check each parsed filter
        for curr_filter in compare_filters:
            self.assertEqual(curr_filter.filter_type, self.filter_type_factory.get(self.type_name_compare))
            comparator = curr_filter.comparator
            symbol = comparator.symbol

            # We have parsed in the right comparison operator and value
            self.assertTrue(symbol in comparator_val_dict)
            self.assertEqual(curr_filter.val, int(comparator_val_dict[symbol]))

        # Now we test that whether the queryset can be updated correctly
        self._set_parsed_filters(filters)
        self.helper.parse("", self.queryset)
        filter_type = self.filter_type_factory.get(self.type_name_compare)

        # For this case we only test with one field name
        field_name = filter_type.tag.field_names[0]
        # So we expect these comparison operators to be translated into the correct filter names
        # Note that this also indirectly test handling multiple search query tokens
        # (i.e. filters should be `AND` together)
        expected_q = Q(**{f'{field_name}__lt': 1}) \
            & Q(**{f'{field_name}__lte': 2}) \
            & Q(**{f'{field_name}__gt': 3}) \
            & Q(**{f'{field_name}__gte': 4}) \
            & Q(**{f'{field_name}': 5})

        self.assertEqual(self.exposed_filter_q, expected_q)

    def test_parse_raw_query_case_sensitivity_valid(self):
        """
        Test we can parse raw query with valid number/date 'comparison' filter
        """
        val = "some_string"
        # Also add a string filter so we can check whether enabling case sensitivity works
        raw_query = f'case:true {self.type_name_contains}:{val}'
        filters = self.helper._parse_raw_query(raw_query)

        # Check at high level we have the right number of filters
        self.assertTrue(FilterType.CASE_SENSITIVE in filters)
        case_filter = filters[FilterType.CASE_SENSITIVE][0]

        # We have parsed in the right filter type and value.
        self.assertEqual(case_filter.filter_type, self.filter_type_factory.get(FilterType.CASE_SENSITIVE))
        self.assertTrue(case_filter.val, True)

        # Now we test that whether the queryset can be updated correctly
        self._set_parsed_filters(filters)
        self.helper.parse("", self.queryset)
        filter_type = self.filter_type_factory.get(self.type_name_contains)

        # For this case we only test with one field name
        field_name = filter_type.tag.field_names[0]
        # By enabling case sensitivity we should not have `icontains`
        expected_q = Q(**{f'{field_name}__contains': val})

        self.assertEqual(self.exposed_filter_q, expected_q)

    def test_parse_raw_query_multiple_field_names(self):
        # Set up filter tag and type which supports checking value with multiple fields
        filter_tag_name = "TAG_MULTIPLE_FIELDS_STRING"
        string_field_names = ("string_field_1", "string_field_2", "string_field_2")
        self.filter_tag_factory.register(FilterTag(
            name=filter_tag_name,
            value_parser=str,
            field_names=string_field_names)
        )

        filter_type_name = "either_field_contains"
        self.filter_type_factory.register(FilterType(
            name=filter_type_name,
            tag=self.filter_tag_factory.get(filter_tag_name),
            method=self.filter_method_factory.get(FilterMethod.CONTAINS),
            description="test description"
        ))

        # The focus of this test is to test multiple fields (for filter tag)
        val = "some_string"
        filters = self.helper._parse_raw_query(f'{filter_type_name}:{val}')
        contains_filter = filters[filter_type_name][0]

        # Now we test that whether the queryset can be updated correctly
        self._set_parsed_filters(filters)
        self.helper.parse("", self.queryset)

        # This time we need to iterate through field_names as we have multiple ones
        expected_q = Q()
        for field_name in string_field_names:
            q_arg = f'{field_name}__icontains'
            # Same filter on different field names should be `OR` together
            expected_q = expected_q | Q(**{q_arg: val})

        self.assertEqual(self.exposed_filter_q, expected_q)

    def test_parse_raw_query_invalid_type(self):
        """
        Test we can detect invalid filter type from raw query
        """
        with self.assertRaises(InvalidSearchQuery):
            self.helper._parse_raw_query(query_raw="unsupported_type:1")

    def test_parse_raw_query_invalid_comparison_operator(self):
        """
        Test we can detect invalid comparison operator from raw query
        """
        with self.assertRaises(InvalidSearchQuery):
            self.helper._parse_raw_query(query_raw=f'{self.type_name_compare}:!=1')  # Use an inquality which is invalid
