# -*- coding: utf-8 -*-
"""Django settings for running data portal integration tests

Usage:
- export DJANGO_SETTINGS_MODULE=data_portal.settings.it
"""
import aws_xray_sdk as xray
from environ import Env

from .base import *  # noqa

DATABASES = {
    'default': Env.db_url_config(
        os.getenv('PORTAL_DB_URL', 'mysql://root:root@localhost:3306/data_portal')
    )
}

# turn off xray for CI test
xray.global_sdk_config.set_sdk_enabled(False)
