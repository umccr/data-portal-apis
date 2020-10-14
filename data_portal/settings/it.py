# -*- coding: utf-8 -*-
"""Django settings for running data portal integration tests

Usage:
- export DJANGO_SETTINGS_MODULE=data_portal.settings.it
"""

from environ import Env

from .base import *  # noqa

DATABASES = {
    'default': Env.db_url_config(
        os.getenv('PORTAL_DB_URL', 'mysql://root:root@localhost:3306/data_portal')
    )
}
