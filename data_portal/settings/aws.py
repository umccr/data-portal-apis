# -*- coding: utf-8 -*-
"""AWS Django settings for data portal

Usage:
- export DJANGO_SETTINGS_MODULE=data_portal.settings.aws
"""
from environ import Env

from utils import libssm
from .base import *  # noqa

SECRET_KEY = libssm.get_secret('/data_portal/backend/django_secret_key')

DEBUG = False

db_conn_cfg = Env.db_url_config(libssm.get_secret('/data_portal/backend/full_db_url'))
db_conn_cfg['OPTIONS'] = {
    'max_allowed_packet': MYSQL_CLIENT_MAX_ALLOWED_PACKET,
}

DATABASES = {
    'default': db_conn_cfg
}
