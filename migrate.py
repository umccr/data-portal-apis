# -*- coding: utf-8 -*-
"""migrate lambda module

Convenience AWS lambda handler for Django database migration command hook
"""
try:
    import unzip_requirements
except ImportError:
    pass

import os

from django.core.management import execute_from_command_line

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')


def handler(event, context) -> str:
    execute_from_command_line(['./manage.py', 'migrate'])
    return 'Migration complete.'
