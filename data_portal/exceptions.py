class InvalidQueryParameter(Exception):
    """
    Raised when query parameter is deemed as invalid
    """
    def __init__(self, key: str, val: str = '', error: str = '', *args: object) -> None:
        super().__init__('Invalid query parameter: %s - %s\nError: %s' % (key, val, error), *args)


class InvalidSearchQuery(Exception):
    """
    Raised when the search query is deemed as invalid.
    """
    def __init__(self, query: str, error: str = '', *args: object) -> None:
        super().__init__('Invalid search query: %s\nError: %s' % (query, error), *args)


class InvalidComparisonOperator(Exception):
    """
    Raised when the comparison operator is deemed as invalid.
    """
    def __init__(self, operator_val_raw: str, *args: object) -> None:
        super().__init__('Invalid comparison operator in "%s"' % operator_val_raw, *args)


class InvalidFilterValue(Exception):
    """
    Raised when filter value is invalid (i.e. can not be converted to the true variable type)
    """
    def __init__(self, val_raw: str, *args: object) -> None:
        super().__init__('Invalid filter value: %s' % val_raw, *args)


class RandSamplesTooLarge(Exception):
    """
    Raised when the requested number of random samples is too large
    """
    def __init__(self, limit: int, *args: object) -> None:
        super().__init__(
            'Requested number of random samples is too large. Max %d.' % limit,
            *args
        )

