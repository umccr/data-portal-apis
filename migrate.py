#!/usr/bin/env python
try:
  import unzip_requirements
except ImportError:
  pass

import os
import sys


def handler(event, context):
    """
    Handler for running DB migrations
    """
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(['manage.py', 'migrate'])
    return True


if __name__ == '__main__':
    handler(None, None)
