from collections import defaultdict
from enum import Enum
from typing import Tuple, Callable, Any, Dict, List, Optional
from django.db.models import QuerySet, Q

from data_portal.exceptions import InvalidComparisonOperator, InvalidFilterValue, InvalidSearchQuery


class ComparisonOperator(Enum):
    """
    Enum for a comparison operator used, allowing conversion from symbol strings (user input) to Django operators
    """
    # OPERATOR = (query symbol, Django query operator)
    EQUAL = ('=', None)
    GREATER_THAN = ('>', 'gt')
    LESS_THAN = ('<', 'lt')
    GREATER_OR_EQUAL_THAN = ('>=', 'gte')
    LESS_OR_EQUAL_THAN = ('<=', 'lte')

    @staticmethod
    def all_comparator_symbols():
        """
        Get all supported comparator symbols, used for query validation.
        :return: list of comparator symbol strings
        """
        return [o.value[0] for o in ComparisonOperator]

    @property
    def symbol(self) -> str:
        """
        :return: string symbol of the operator
        """
        return self.value[0]

    @property
    def name(self) -> str:
        """
        :return: name of the operator
        """
        return self.value[1]


class FilterMethod:
    """
    Representing a filter method
    For any new (base) method, define the name constant below.
    Extend this class for additional methods which are specific to a search view.
    """
    # METHOD_NAME = 'UNIQUE_STRING_IDENTIFIER'
    ENDS_WITH = 'ENDS_WITH'
    CONTAINS = 'CONTAINS'
    COMPARE = 'COMPARE'
    CASE_SENSITIVITY = 'CASE_SENSITIVITY'

    def __init__(self, name: str, is_global: bool) -> None:
        """
        :param name: name of the filter method (using a defined name constant above)
        :param is_global: whether this method is a global filter (rule)
        """
        self.name = name
        self.is_global = is_global


class FilterMethodFactory:
    """
    Factory class storing all supported filter methods.
    Extend this class for additional methods which are specific to a search view.
    """
    def __init__(self) -> None:
        self._methods = {}

        # Register base filter methods
        self.register(FilterMethod(FilterMethod.ENDS_WITH, False))
        self.register(FilterMethod(FilterMethod.CONTAINS, False))
        self.register(FilterMethod(FilterMethod.COMPARE, False))
        self.register(FilterMethod(FilterMethod.CASE_SENSITIVITY, True))

    def register(self, filter_method: FilterMethod) -> None:
        """
        Register a filter method into the factory
        :param filter_method: new filter method to be added in
        """
        self._methods[filter_method.name] = filter_method

    def get(self, filter_method_name: str) -> FilterMethod:
        """
        Get the filter name with the specified name
        :param filter_method_name: method name
        """
        return self._methods.get(filter_method_name, None)


class FilterTag:
    CASE = 'CASE'

    def __init__(self, name: str, value_parser: Callable[[str], Any], field_names: Optional[Tuple]) -> None:
        """
        :param name: name of the filter tag (using a defined name constant above)
        :param value_parser: parser function that converts raw value in str to the real type
        :param field_names: field names used in Django query, in a Tuple.
                            Note that one filter can be applied on multiple field names
                            (currently only OR relation supported)
        """
        self.name = name
        self.value_parser = value_parser
        self.field_names = field_names


class FilterTagFactory:
    def __init__(self) -> None:
        self._fields = {}

        # Register base filter tags
        self.register(FilterTag(FilterTag.CASE, lambda c: c.lower() == 'true', None))

    def register(self, field: FilterTag):
        self._fields[field.name] = field

    def get(self, name: str) -> FilterTag:
        return self._fields.get(name, None)


class FilterType:
    """
    Represents a filter field with a specific filter type.
    """
    CASE_SENSITIVE = 'case'

    def __init__(
        self,
        name: str,
        tag: FilterTag,
        method: FilterMethod,
        description: str
    ) -> None:
        self.name = name
        self.tag = tag
        self.method = method
        self.description = description


class FilterTypeFactory:
    """
    Factory class storing supported filter types
    """
    def __init__(self, filter_tag_factory: FilterTagFactory, filter_method_factory: FilterMethodFactory) -> None:
        self._filter_types = {}
        self._default = None
        self.filter_tag_factory = filter_tag_factory
        self.filter_method_factory = filter_method_factory

        self.register(FilterType(
            FilterType.CASE_SENSITIVE,
            filter_tag_factory.get(FilterTag.CASE),
            filter_method_factory.get(FilterMethod.CASE_SENSITIVITY),
            'Defines case sensitivity for string comparison. Default to false'
        ))

    def register(self, filter_type: FilterType) -> None:
        self._filter_types[filter_type.name] = filter_type

    def set_default(self, filter_type_name: str) -> None:
        self._default = self._filter_types.get(filter_type_name)

    def get(self, filter_type_name: str) -> FilterType:
        return self._filter_types.get(filter_type_name, None)

    def get_default(self) -> FilterType:
        return self._default


class Filter:
    """
    Represents a single filter query
    """
    filter_type: FilterType
    val: Any
    comparator: ComparisonOperator

    def __init__(self, filter_type: FilterType, comparator_val_raw: str) -> None:
        """
        Construct a filter query from the specified FilterFieldType and raw comparator+value string
        :param filter_type: the FilterType this query corresponds to
        :param comparator_val_raw: raw comparator+value string, comparator may be empty.
        """
        super().__init__()
        self.filter_type = filter_type

        comparator = None
        if filter_type.method.name == FilterMethod.COMPARE:
            # Check whether comparison operator is valid and find it if it's valid
            comparators = list(filter(
                lambda o: comparator_val_raw.startswith(o.symbol),
                [o for o in ComparisonOperator]
            ))

            if len(comparators) == 0:
                raise InvalidComparisonOperator(comparator_val_raw)
            elif len(comparators) == 2:
                # Use the longer one for cases like < and <=
                comparator = max(comparators, key=lambda o: len(o.symbol))
            else:
                # Should have exactly one match.
                comparator = comparators[0]

        # Calculate the len of comparison operator string
        comparator_len = len(comparator.symbol) if comparator is not None else 0

        # Once we have the operator, the val is the remaining string
        # Convert the val to the true variable type
        val_raw = comparator_val_raw[comparator_len:]
        try:
            self.val = filter_type.tag.value_parser(val_raw)
        except ValueError:
            raise InvalidFilterValue(val_raw)

        self.comparator = comparator


class SearchQueryHelper:
    def __init__(
        self,
        filter_type_factory: FilterTypeFactory,
        filter_tag_factory: FilterTagFactory,
        filter_method_factory: FilterMethodFactory
    ) -> None:
        self._filter_type_factory = filter_type_factory
        self._filter_tag_factory = filter_tag_factory
        self._filter_method_factory = filter_method_factory
        self._case_sensitive_prefix = ''

    def _parse_raw_query(self, query_raw: str) -> Dict[str, List[Filter]]:
        """
        Parse raw query to defined filters.
        :param query_raw: raw query
        :except InvalidSearchQuery: search query is found to be invalid
        :return: defined filters
        """
        # Remove spaces around and split into each individual filter
        filters_raw = query_raw.strip().split(' ')

        filters: Dict[str, List[Filter]] = defaultdict(list)

        for filter_raw in filters_raw:
            # Remove spaces around and split into left and right
            tokens = filter_raw.strip().split(':')

            if len(tokens) > 2:
                raise InvalidSearchQuery(query_raw, 'filter "%s" is invalid' % filter_raw)

            if len(tokens) == 1:
                # Default filter
                val = tokens[0]
                default_filter = self._filter_type_factory.get_default()

                try:
                    filters[default_filter.name].append(Filter(default_filter, comparator_val_raw=val))
                except InvalidFilterValue as e:
                    raise InvalidSearchQuery(query_raw, str(e))

            if len(tokens) == 2:
                # Non-default filter
                filter_type_name = tokens[0]
                comparator_val = tokens[1]

                # Check whether the filter id is valid
                filter_type = self._filter_type_factory.get(filter_type_name)
                if filter_type is None:
                    raise InvalidSearchQuery(query_raw, 'invalid filter type "%s"' % filter_type_name)

                try:
                    filters[filter_type_name].append(Filter(filter_type, comparator_val_raw=comparator_val))
                except InvalidComparisonOperator as e:
                    raise InvalidSearchQuery(query_raw, str(e))
                except InvalidFilterValue as e:
                    raise InvalidSearchQuery(query_raw, str(e))

        return filters

    def apply_global_filters(self, filters: Dict[str, List[Filter]], queryset: QuerySet) -> QuerySet:
        """
        Apply global filters, override this if needed
        :param filters: parsed filters from raw query string
        :param queryset: base queryset
        :return: updated queryset
        """
        case_sensitive_name = FilterType.CASE_SENSITIVE
        case_sensitive = case_sensitive_name in filters and filters[case_sensitive_name][0].val is True

        if not case_sensitive:
            self._case_sensitive_prefix = 'i'

        return queryset

    def parse(self, query_raw: str, base_queryset: QuerySet) -> QuerySet:
        """
        Parse raw query into Django QuerySet
        :param query_raw: raw query
        :param base_queryset: equivalent base QuerySet
        :except InvalidSearchQuery: search query is found to be invalid
        :return: parsed QuerySet
        """
        filters = self._parse_raw_query(query_raw)

        # Base query set
        queryset = base_queryset
        # Apply global filters first
        queryset = self.apply_global_filters(filters, queryset)

        # Now apply non-global filters
        for filter_type_id, filter_list in filters.items():
            # Ignore global filters
            filter_method = self._filter_type_factory.get(filter_type_id).method
            if filter_method.is_global:
                continue

            for curr_filter in filter_list:
                method = curr_filter.filter_type.method
                tag = curr_filter.filter_type.tag
                val = curr_filter.val

                filter_q = Q()

                for field_name in tag.field_names:
                    if method.name == FilterMethod.COMPARE:
                        comparator = curr_filter.comparator
                        if comparator.name is None:
                            # No comparator for exact comparison
                            filter_arg = field_name
                        else:
                            # Correspond to {field_name}__{operator}
                            filter_arg = f'{field_name}__{comparator.name}'
                    elif method.name == FilterMethod.CONTAINS:
                        # Correspond to {field_name}__{(i)contains}
                        filter_arg = f'{field_name}__{self._case_sensitive_prefix + "contains"}'
                    elif method.name == FilterMethod.ENDS_WITH:
                        # Correspond to {field_name}__{(i)ends_with}
                        filter_arg = f'{field_name}__{self._case_sensitive_prefix + "endswith"}'
                    else:
                        # Extra safe guard for unexpected/unsupported filter type
                        raise Exception('Unexpected filter type: %s' % curr_filter.filter_type.name)

                    filter_kwarg = {filter_arg: val}
                    filter_q = filter_q | Q(**filter_kwarg)

                # Append new filter
                queryset = queryset.filter(filter_q)

        # We only want distinct results as we may have performed table joins
        return queryset.distinct()
