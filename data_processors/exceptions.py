class UnexpectedLIMSDataFormatException(Exception):
    """
    Raised when we encounter unexpected LIMS data format, such as duplicate row identifiers
    """
    def __init__(self, message) -> None:
        super().__init__('Unexpected LIMS data format - ' + message)
