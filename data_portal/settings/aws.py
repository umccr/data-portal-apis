# -*- coding: utf-8 -*-
"""AWS Django settings for data portal

Usage:
- export DJANGO_SETTINGS_MODULE=data_portal.settings.aws
"""
from environ import Env

from utils import libssm
from .base import *  # noqa

SECRET_KEY = libssm.get_secret(os.environ['SSM_KEY_NAME_DJANGO_SECRET_KEY'])

DEBUG = False

DATABASES = {
    'default': Env.db_url_config(libssm.get_secret(os.environ['SSM_KEY_NAME_FULL_DB_URL']))
}
