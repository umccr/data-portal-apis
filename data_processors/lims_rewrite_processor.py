try:
  import unzip_requirements
except ImportError:
  pass

import io
import os, django
# We need to set up django app first
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# All other imports should be placed below
import logging
from data_processors.persist_lims_data import persist_lims_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)




def handler(event, context):
    # Trigger rewrite directly
    return persist_lims_data()
