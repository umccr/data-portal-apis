from datetime import datetime


def parse_last_modified_date(date_raw: str) -> datetime:
    """
    Date string parser for last_modified_date attribute of S3 object (event data)
    :param date_raw: date in raw string
    """
    return datetime.strptime(date_raw, '%Y-%m-%d')


def parse_lims_timestamp(date_raw: str) -> datetime:
    """
    Date string parser for timestamp column of LIMS data
    :param date_raw: date in raw string
    """
    return datetime.strptime(date_raw, '%Y-%m-%d')
