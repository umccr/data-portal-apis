from unittest.mock import MagicMock, Mock

from django.db.models import Q
from django.db.models.query import QuerySet
from django.test import TestCase

from data_portal.exceptions import InvalidSearchQuery
from data_portal.search_query import SearchQueryHelper, FilterMethodFactory, FilterTagFactory, FilterTypeFactory, \
    FilterTag, FilterType, FilterMethod


class SearchQueryTests(TestCase):
    """
    Test cases for (base) search query processing functionality
    """
    def setUp(self) -> None:
        # Create mocked queryset so that we can record what has been passed to filter() method
        self.queryset = QuerySet()
        self.queryset.filter = Mock(side_effect=self._record_filter_arguments)
        self.filter_arguments = []

        # Set up some sample filter methods, tags, types and factories for testing
        self.filter_method_factory = FilterMethodFactory()
        self.filter_tag_factory = FilterTagFactory()
        self.filter_type_factory = FilterTypeFactory(
            filter_tag_factory=self.filter_tag_factory,
            filter_method_factory=self.filter_method_factory
        )

        filter_tag_string_name = "TAG_STRING"
        string_field_names = ("string_field_1", "string_field_2")
        self.filter_tag_factory.register(FilterTag(
            name=filter_tag_string_name,
            value_parser=str,
            field_names=string_field_names)
        )

        filter_tag_int_name = "TAG_INT"
        string_field_names = ("integer_field_1", "integer_field_2")
        self.filter_tag_factory.register(FilterTag(
            name=filter_tag_int_name,
            value_parser=str,
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

    def _record_filter_arguments(self, filter_arguments) -> None:
        """
        Side effect method used for queryset object, recording the arguments passed into queryset.filter()
        """
        self.filter_arguments.append(filter_arguments)

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
        val_1 = "some_string_1"
        val_2 = "some_string_2"
        filters = self.helper._parse_raw_query(f'{self.type_name_contains}:{val_1} {self.type_name_contains}:{val_2}')

        # Check at high level we have the right number of filters
        self.assertTrue(self.type_name_contains in filters)
        contains_filters = filters[self.type_name_contains]
        self.assertEqual(len(contains_filters), 2)

        # Check each parsed filter
        for curr_filter in contains_filters:
            # We have parsed in the right filter type and value.
            self.assertEqual(curr_filter.filter_type, self.filter_type_factory.get(self.type_name_compare))
            self.assertTrue(curr_filter.val in [val_1])

    def test_parse_raw_query_ends_with(self):
        """
        Test we can parse raw query with valid string 'ends with' filter
        """
        val_1 = "some_string_1"
        val_2 = "some_string_2"
        filters = self.helper._parse_raw_query(f'{self.type_name_ends_with}:{val_1} {self.type_name_ends_with}:{val_2}')

        # Check at high level we have the right number of filters
        self.assertTrue(self.type_name_ends_with in filters)
        contains_filters = filters[self.type_name_ends_with]
        self.assertEqual(len(contains_filters), 2)

        # Check each parsed filter
        for curr_filter in contains_filters:
            # We have parsed in the right filter type and value.
            self.assertEqual(curr_filter.filter_type, self.filter_type_factory.get(self.type_name_ends_with))
            self.assertTrue(curr_filter.val in [val_1])

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

    def test_parse_raw_query_case_sensitivity_valid(self):
        """
        Test we can parse raw query with valid number/date 'comparison' filter
        """
        raw_query = "case:true"
        filters = self.helper._parse_raw_query(raw_query)

        # Check at high level we have the right number of filters
        self.assertTrue(FilterType.CASE_SENSITIVE in filters)
        case_filter = filters[FilterType.CASE_SENSITIVE][0]

        # We have parsed in the right filter type and value.
        self.assertEqual(case_filter.filter_type, self.filter_type_factory.get(FilterType.CASE_SENSITIVE))
        self.assertTrue(case_filter.val, True)

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
            self.helper._parse_raw_query(query_raw=f'{self.type_name_compare}:<>1')
