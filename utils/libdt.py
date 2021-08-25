from datetime import datetime, timezone


def get_utc_now_ts():
    return int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())


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


def serializable_datetime(dt) -> str:
    """
    Convert to serializable datetime iso format string, e.g. use in AWS Lambda call dict return object
    :param dt:
    :return:
    """
    if isinstance(dt, str):
        return dt

    if isinstance(dt, datetime):
        _dt: datetime = dt
        return _dt.isoformat()


def folder_friendly_timestamp(dt: datetime) -> str:
    """
    Get a folder friendly timestamp of the format
    :param dt:
    :return:
    """
    return dt.strftime("%Y-%m-%d__%H-%M-%S")
