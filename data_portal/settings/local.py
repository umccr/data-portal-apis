# -*- coding: utf-8 -*-
"""local development Django settings for data portal

Usage:
- export DJANGO_SETTINGS_MODULE=data_portal.settings.local
"""
import sys

from environ import Env

from .base import *  # noqa


db_conn_cfg = Env.db_url_config(os.getenv('PORTAL_DB_URL', 'mysql://root:root@localhost:3306/data_portal'))
db_conn_cfg['OPTIONS'] = {
    'max_allowed_packet': MYSQL_CLIENT_MAX_ALLOWED_PACKET,
}

DATABASES = {
    'default': db_conn_cfg
}

INSTALLED_APPS += ('django_extensions',)

RUNSERVER_PLUS_PRINT_SQL_TRUNCATE = sys.maxsize
