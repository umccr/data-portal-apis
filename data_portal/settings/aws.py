# -*- coding: utf-8 -*-
"""AWS Django settings for data portal

Usage:
- export DJANGO_SETTINGS_MODULE=data_portal.settings.aws
"""
import copy

from environ import Env
from libumccr.aws import libssm

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

CORS_ORIGIN_ALLOW_ALL = False
CORS_ALLOW_CREDENTIALS = False

CORS_ALLOWED_ORIGINS = [
    'https://data.umccr.org',
    'https://data.prod.umccr.org',
    'https://data.dev.umccr.org',
]

CSRF_TRUSTED_ORIGINS = copy.deepcopy(CORS_ALLOWED_ORIGINS)
