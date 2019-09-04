from collections import defaultdict
from enum import Enum
from typing import List, Dict

from django.db.models import QuerySet

from data_portal.models import S3Object


class FilterType(Enum):
    END_WITH = 'end_with'
    INCLUDE = 'include'
    EXACT = 'exact'
    COMPARE = 'compare'
    GLOBAL = 'global'


DEFAULT_FILTER_FIELD = 'key'
DEFAULT_FILTER_METHOD = FilterType.EXACT
FILTERS = {
    'key': {
        'field': 'key',
        'filter_types': [FilterType.INCLUDE, FilterType.EXACT, FilterType.END_WITH],
        'val_type': str,
    }
}

S3Object.objects.filter()

COMPARISON_OPERATORS = [
    '='
    '>'
    '<'
    '>='
    '<='
]


class InvalidSearchQuery(Exception):
    def __init__(self, query: str, *args: object) -> None:
        super().__init__('Invalid search query %s' % query, *args)


class Filter:
    def __init__(self, field: str, method: FilterType, val: str, comparator: str = None) -> None:
        super().__init__()


class SearchQueryHelper:
    @staticmethod
    def parse(query: str) -> QuerySet:
        filters_raw = query.strip().split(' ')

        queryset = S3Object.objects
        filters: Dict[str, List[Filter]] = defaultdict(list)

        for filter_raw in filters_raw:
            tokens = filter_raw.split(':')

            if len(tokens) > 2:
                raise InvalidSearchQuery(query)

            if len(tokens) == 1:
                # Default filter
                val = tokens[0]
                filters[DEFAULT_FILTER_FIELD].append(Filter(DEFAULT_FILTER_FIELD, DEFAULT_FILTER_METHOD, val))

            if len(tokens) == 2:
                # Default filter
                key = tokens[0]
                comparator_val = tokens[1]

                if key not in FILTERS:
                    raise InvalidSearchQuery(query)

                key_var_type = FILTERS[key]['var_type']
                comparator = None

                if FilterType.COMPARE in FILTERS[key]['filter_types']:
                    comparator = None
                    for o in COMPARISON_OPERATORS:
                        if o in comparator_val:
                            comparator = o
                    if comparator is None:
                        raise InvalidSearchQuery(query)

                comparator_len = len(comparator) if comparator is not None else 0
                val = key_var_type(comparator_val[comparator_len:])
                filters[key].append(Filter(key, DEFAULT_FILTER_METHOD, val, comparator))

        return queryset