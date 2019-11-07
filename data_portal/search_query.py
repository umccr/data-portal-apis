from collections import defaultdict
from enum import Enum
from typing import Tuple, Callable, Any, Dict, List, Optional, Union, Type
from django.db.models import QuerySet, Q

from data_portal.exceptions import InvalidComparisonOperator, InvalidFilterValue, InvalidSearchQuery


class ComparisonOperator(Enum):
    """
    Enum for the comparison operator supported for COMPARISON FilterMethod,
    storing the conversion table from symbol strings (user input) to Django operators.
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
        Get the symbol of current operator
        """
        return self.value[0]

    @property
    def name(self) -> str:
        """
        Get the name of current operator (in Django querying)
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
        Configure a new supported filter method.
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
    _methods: Dict[str, FilterMethod]

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
    """
    Representing a filter tag, in which value parser will be used and apply on the model field names
    Extend this class for additional tags which are specific to a search view.
    """
    # TAG_NAME = 'UNIQUE_STRING_IDENTIFIER'
    CASE = 'CASE'

    def __init__(
        self,
        name: str,
        value_parser: Union[Callable[[str], Any], Type],
        field_names: Optional[Tuple]
    ) -> None:
        """
        Configure a new supported filter tag.
        :param name: name of the filter tag (using a defined name constant above)
        :param value_parser: parser function that converts raw value in str to the real type
                             which can be either a function or a primitive type (e.g. str, and we will do `str(val)`)
        :param field_names: field names used in Django query, in a Tuple.
                            Note that one filter can be applied on multiple field names
                            (currently only OR relation supported)
        """
        self.name = name
        self.value_parser = value_parser
        self.field_names = field_names


class FilterTagFactory:
    """
    Factory class storing all supported filter tags.
    Extend this class for additional tags which are specific to a search view.
    """
    _tags: Dict[str, FilterTag]

    def __init__(self) -> None:
        self._tags = {}

        # Register base filter tags
        self.register(FilterTag(FilterTag.CASE, lambda c: c.lower() == 'true', None))

    def register(self, filter_tag: FilterTag):
        """
        Register a filter tag into the factory
        :param filter_tag: new filter tag to be added in
        """
        self._tags[filter_tag.name] = filter_tag

    def get(self, tag_name: str) -> FilterTag:
        """
        Get the filter tag with the specified name
        :param tag_name: tag name
        """
        return self._tags.get(tag_name, None)


class FilterType:
    """
    Represents a filter field with a specific filter type.
    Extend this class for additional types which are specific to a search view.
    """
    # TAG_NAME = 'lowercase_unique_identifier' (used in query string - [filter_type]:[operator][filter_val])

    CASE_SENSITIVE = 'case'

    def __init__(
        self,
        name: str,
        tag: FilterTag,
        method: FilterMethod,
        description: str
    ) -> None:
        """
        Configure a new supported filter type.
        :param name: name of the filter type (used in query string - [filter_type]:[operator][filter_val]
        :param tag: the underlying filter tag supporting this filter type.
                    note that one filter tag can be used by multiple filter types.
        :param method: the underlying filter method supporting this filter type.
        :param description: description of this filter type, which can also be used in the future for dynamic
                            querying help text retrieval.
        """
        self.name = name
        self.tag = tag
        self.method = method
        self.description = description


class FilterTypeFactory:
    """
    Factory class storing supported filter types
    """
    _filter_types: Dict[str, FilterType]

    def __init__(self, filter_tag_factory: FilterTagFactory, filter_method_factory: FilterMethodFactory) -> None:
        """
        :param filter_tag_factory: used for retrieving the corresponding filter tag objects
        :param filter_method_factory: used for retrieving the corresponding filter method objects
        """
        self._filter_types = {}
        self._default = None
        self.filter_tag_factory = filter_tag_factory
        self.filter_method_factory = filter_method_factory

        # Register base filter tags
        self.register(FilterType(
            FilterType.CASE_SENSITIVE,
            filter_tag_factory.get(FilterTag.CASE),
            filter_method_factory.get(FilterMethod.CASE_SENSITIVITY),
            'Defines case sensitivity for string comparison. Default to false'
        ))

    def register(self, filter_type: FilterType) -> None:
        """
        Register a filter type into the factory
        :param filter_type: new filter type to be added in
        """
        self._filter_types[filter_type.name] = filter_type

    def set_default(self, filter_type_name: str) -> None:
        """
        Set the default filter type to the filter type name (which must already exists in `_filter_types`)
        :param filter_type_name: name of the filter type
        """
        self._default = self._filter_types.get(filter_type_name)

    def get(self, filter_type_name: str) -> FilterType:
        """
        Get the filter type with the specified name
        :param filter_type_name: name of the filter type
        """
        return self._filter_types.get(filter_type_name, None)

    def get_default(self) -> FilterType:
        """
        Get the default filter type (for the case when the input only contains a value [v], so no [t]:[o][v] and we can
        refer to the default filter type.
        """
        return self._default


class Filter:
    """
    Represents a single filter query: [filter_type]:[operator][filter_val]
    Note that operator is optional depending on the filter type.
    """
    filter_type: FilterType
    comparator: ComparisonOperator
    val: Any  # Value converted from raw value

    def __init__(self, filter_type: FilterType, comparator_val_raw: str) -> None:
        """
        Construct a filter query from the specified FilterFieldType and raw comparator+value string
        :param filter_type: the FilterType this query corresponds to
        :param comparator_val_raw: raw comparator+value string, comparator may be empty.
        :raise InvalidComparisonOperator: comparison operator is in invalid format
        :raise InvalidFilterValue: filter value is in invalid format
        """
        super().__init__()
        self.filter_type = filter_type

        comparator = None

        # Only checks for comparison operator if we are comparing
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
    """
    Helper class for search query text parsing and processing
    """
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
        :except InvalidSearchQuery: search query string is invalid
        :return: parsed filters, list of Filter objects mapped by filter type name.
                 note that this also means we support applying multiple filters of the same type.
        """
        # Remove spaces around and split into each individual filter
        filters_raw = query_raw.strip().split(' ')

        filters: Dict[str, List[Filter]] = defaultdict(list)

        for filter_raw in filters_raw:
            # Ignore empty string raw filter
            if filter_raw == '':
                continue

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
                # Non-default filter, so we expect a filter type name
                filter_type_name = tokens[0]
                comparator_val = tokens[1]  # [comparator(optional)][filter value]

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
        # Apply case sensitivity preference, default is case insensitive.
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

            # Note that we support multiple filter conditions of the same type by `AND` each filter condition.
            for curr_filter in filter_list:
                method = curr_filter.filter_type.method
                tag = curr_filter.filter_type.tag
                val = curr_filter.val

                filter_q = Q()

                # We also support comparison to multiple fields, and we `OR` each filter condition.
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
