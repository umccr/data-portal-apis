from datetime import datetime


def parse_last_modified_date(date_raw: str):
    return datetime.strptime(date_raw, '%Y-%m-%d')


def parse_lims_timestamp(date_raw: str):
    return datetime.strptime(date_raw, '%d/%m/%y')
