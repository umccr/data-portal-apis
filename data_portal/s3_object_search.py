from collections import defaultdict
from enum import Enum
from typing import List, Dict, Any, Callable, Tuple
from django.db.models import QuerySet, Q

from data_portal.exceptions import InvalidComparisonOperator, InvalidSearchQuery, InvalidFilterValue
from utils.datetime import parse_last_modified_date


class ComparisonOperator(Enum):
    """
    Enum for a comparison operator used, allowing conversion from symbol strings (user input) to Django operators
    """
    # OPERATOR = (symbol, Django operator)
    EQUAL = ('=', None)
    GREATER_THAN = ('>', 'gt')
    LESS_THAN = ('<', 'lt')
    GREATER_OR_EQUAL_THAN = ('>=', 'gte')
    LESS_OR_EQUAL_THAN = ('<=', 'lte')

    @staticmethod
    def all_comparator_symbols():
        return [o.value[0] for o in ComparisonOperator]

    @property
    def filter_type_symbol(self) -> str:
        return self.value[0]

    @property
    def filter_type_name(self) -> str:
        return self.value[1]


class FilterType(Enum):
    """
    Enum for all supported filer types.
    If there is any new filter type, S3ObjectSearchQueryHelper.parse() MUST be updated.
    """
    # Note: the value of each enum is meaningless
    ENDS_WITH = 'ENDS_WITH'
    CONTAINS = 'CONTAINS'
    COMPARE = 'COMPARE'
    GLOBAL_CASE_SENSITIVITY = 'GLOBAL_CASE_SENSITIVITY'
    GLOBAL_LINKED_WITH_LIMS = 'GLOBAL_LINKED_WITH_LIMS'

class FilterField(Enum):
    """
    Enum for all supported filter fields.
    """
    # FIELD = (model_field, value_parser, model_class)
    # Important Note:
    # - model_field:
    #       - MUST correspond to a valid field in the corresponding model
    #       - if model_class is S3LIMS, MUST attach the associated table name before the actual field.
    #         e.g. s3lims__lims_row__illumina_id.
    #         (Note that Django will then 'inner join's the S3Object and LIMS table)
    # - model_class: either S3Object or S3LIMS. S3Object is our 'base' class.

    KEY = (('key',), str)
    SIZE = (('size',), int)
    LAST_MODIFIED_DATE = (('last_modified_date',), parse_last_modified_date)
    SUBJECT_ID = (('s3lims__lims_row__subject_id', 's3lims__lims_row__external_subject_id'), str)
    SAMPLE_ID =(('s3lims__lims_row__sample_id',), str)
    CASE = (('case',), lambda c: c.lower() == 'true')
    LINKED = (('linked',), lambda c: c.lower() == 'true')

    @property
    def field_names(self) -> Tuple:
        return self.value[0]

    @property
    def val_parser(self) -> Callable[[Any], Any]:
        return self.value[1]


class FilterFieldType:
    """
    Represents a filter field with a specific filter type.
    """
    def __init__(self, id: str, field: FilterField, type: FilterType, description: str) -> None:
        super().__init__()
        self.id = id
        self.field = field
        self.type = type
        self.description = description


class FilterFieldTypeFactory:
    """
    Factory class for all pre-existing FilterFieldType objects.
    For any new FilterFieldType, you must:
        1. Define the constant for the id below.
        2. Register it in the map below.
    """
    KEY_INCLUDES = 'pathinc'
    KEY_EXTENSION = 'ext'
    FILE_SIZE = 'size'
    LAST_MODIFIED_DATE = 'date'
    SUBJECT_ID = 'subjectid'
    SAMPLE_ID = 'sampleid'
    CASE_SENSITIVE = 'case'
    LINKED_WITH_LIMS = 'linked'

    # The default filter
    DEFAULT = KEY_INCLUDES

    _d = {
        KEY_INCLUDES: FilterFieldType(KEY_INCLUDES, FilterField.KEY, FilterType.CONTAINS, 'File path includes'),
        KEY_EXTENSION: FilterFieldType(KEY_EXTENSION, FilterField.KEY, FilterType.ENDS_WITH, 'File extension'),
        FILE_SIZE: FilterFieldType(FILE_SIZE, FilterField.SIZE, FilterType.COMPARE, 'Compare with file size'),
        LAST_MODIFIED_DATE: FilterFieldType(
            LAST_MODIFIED_DATE,
            FilterField.LAST_MODIFIED_DATE,
            FilterType.COMPARE,
            'Compare with last modified date of the file'
        ),
        SUBJECT_ID: FilterFieldType(
            SUBJECT_ID,
            FilterField.SUBJECT_ID,
            FilterType.CONTAINS,
            'SubjectID/ExternalSubjectID (in LIMS table) includes'
        ),
        SAMPLE_ID: FilterFieldType(
            SAMPLE_ID,
            FilterField.SAMPLE_ID,
            FilterType.CONTAINS,
            'SampleID (in LIMS table) includes'
        ),
        CASE_SENSITIVE: FilterFieldType(
            CASE_SENSITIVE,
            FilterField.CASE,
            FilterType.GLOBAL_CASE_SENSITIVITY,
            'Defines case sensitivity for string comparison. Default to false'
        ),
        LINKED_WITH_LIMS: FilterFieldType(
            LINKED_WITH_LIMS,
            FilterField.LINKED,
            FilterType.GLOBAL_LINKED_WITH_LIMS,
            'The record is linked with at least one LIMS row'
        )
    }

    @staticmethod
    def get(filter_id: str):
        return FilterFieldTypeFactory._d.get(filter_id, None)


class FilterQuery:
    """
    Represents a single filter query
    """
    field_type: FilterFieldType
    val: Any
    comparator: ComparisonOperator

    def __init__(self, field_type: FilterFieldType, comparator_val_raw: str) -> None:
        """
        Construct a filter query from the specified FilterFieldType and raw comparator+value string
        :param field_type: the FilterFieldType this query corresponds to
        :param comparator_val_raw: raw comparator+value string, comparator may be empty.
        """
        super().__init__()
        self.field_type = field_type

        comparator = None
        if field_type.type == FilterType.COMPARE:
            # Check whether comparison operator is valid and find it if it's valid
            comparators = list(filter(
                lambda o: comparator_val_raw.startswith(o.filter_type_symbol),
                [o for o in ComparisonOperator]
            ))

            if len(comparators) == 0:
                raise InvalidComparisonOperator(comparator_val_raw)
            elif len(comparators) == 2:
                # Use the longer one for cases like < and <=
                comparator = max(comparators, key=lambda o: len(o.filter_type_symbol))
            else:
                # Should have exactly one match.
                comparator = comparators[0]

        # Calculate the len of comparison operator string
        comparator_len = len(comparator.filter_type_symbol) if comparator is not None else 0

        # Once we have the operator, the val is the remaining string
        # Convert the val to the true variable type
        val_raw = comparator_val_raw[comparator_len:]
        try:
            self.val = field_type.field.val_parser(val_raw)
        except ValueError:
            raise InvalidFilterValue(val_raw)

        self.comparator = comparator


class S3ObjectSearchQueryHelper:
    @staticmethod
    def _parse_filter_vals(query_raw: str) -> Dict[str, List[FilterQuery]]:
        """
        Parse raw query to defined filters.
        :param query_raw: raw query
        :except InvalidSearchQuery: search query is found to be invalid
        :return: defined filters
        """
        # Remove spaces around and split into each individual filter
        filters_raw = query_raw.strip().split(' ')

        filters: Dict[str, List[FilterQuery]] = defaultdict(list)

        for filter_raw in filters_raw:
            # Remove spaces around and split into left and right
            tokens = filter_raw.strip().split(':')

            if len(tokens) > 2:
                raise InvalidSearchQuery(query_raw, 'filter "%s" is invalid' % filter_raw)

            if len(tokens) == 1:
                # Default filter
                val = tokens[0]
                filter_id = FilterFieldTypeFactory.DEFAULT

                try:
                    filters[filter_id].append(FilterQuery(FilterFieldTypeFactory.get(filter_id), comparator_val_raw=val))
                except InvalidFilterValue as e:
                    raise InvalidSearchQuery(query_raw, str(e))

            if len(tokens) == 2:
                # Non-default filter
                filter_id = tokens[0]
                comparator_val = tokens[1]

                # Check whether the filter id is valid
                filter_field_type = FilterFieldTypeFactory.get(filter_id)
                if filter_field_type is None:
                    raise InvalidSearchQuery(query_raw, 'invalid filter type "%s"' % filter_field_type)

                try:
                    filters[filter_id].append(FilterQuery(filter_field_type, comparator_val_raw=comparator_val))
                except InvalidComparisonOperator as e:
                    raise InvalidSearchQuery(query_raw, str(e))
                except InvalidFilterValue as e:
                    raise InvalidSearchQuery(query_raw, str(e))

        return filters

    @staticmethod
    def parse(query_raw: str, base_queryset: QuerySet) -> QuerySet:
        """
        Parse raw query into Django QuerySet
        :param query_raw: raw query
        :param base_queryset: equivalent base QuerySet
        :except InvalidSearchQuery: search query is found to be invalid
        :return: parsed QuerySet
        """
        filters = S3ObjectSearchQueryHelper._parse_filter_vals(query_raw)

        # Base query set
        queryset = base_queryset

        # First check any global filter
        case_sensitive_id = FilterFieldTypeFactory.CASE_SENSITIVE
        case_sensitive = case_sensitive_id in filters and filters[case_sensitive_id][0].val is True
        case_sensitive_prefix = '' if case_sensitive else 'i'

        linked_with_lims_id = FilterFieldTypeFactory.LINKED_WITH_LIMS
        has_linked_with_lims = linked_with_lims_id in filters

        # Only apply this special filter if we have the filter for linked_with_lims
        if has_linked_with_lims:
            linked_with_lims = filters[linked_with_lims_id][0].val is True

            if linked_with_lims:
                queryset = queryset.filter(s3lims__isnull=False)
            else:
                queryset = queryset.filter(s3lims__isnull=True)

        for filter_id, filter_list in filters.items():
            # Ignore global filters as they have been checked
            if filter_id == case_sensitive_id or filter_id == linked_with_lims_id:
                continue

            for filter in filter_list:
                type = filter.field_type.type
                field = filter.field_type.field
                val = filter.val

                filter_q = Q()

                for field_name in field.field_names:
                    if type == FilterType.COMPARE:
                        comparator = filter.comparator
                        if comparator.filter_type_name is None:
                            # No comparator for exact comparison
                            filter_arg = field_name
                        else:
                            # Correspond to {field_name}__{operator}
                            filter_arg = f'{field_name}__{comparator.filter_type_name}'
                    elif type == FilterType.CONTAINS:
                        # Correspond to {field_name}__{(i)contains}
                        filter_arg = f'{field_name}__{case_sensitive_prefix + "contains"}'
                    elif type == FilterType.ENDS_WITH:
                        # Correspond to {field_name}__{(i)ends_with}
                        filter_arg = f'{field_name}__{case_sensitive_prefix + "endswith"}'
                    else:
                        # Extra safe guard for unexpected/unsupported filter type
                        raise Exception('Unexpected filter type: %s' % type.value)

                    filter_kwarg = {filter_arg: val}
                    filter_q = filter_q | Q(**filter_kwarg)

                # Append new filter
                queryset = queryset.filter(filter_q)

        # We only want distinct results as we may have joined the tables for LIMS field search
        return queryset.distinct()
