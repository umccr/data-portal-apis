try:
    import unzip_requirements
except ImportError:
    pass

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings.base')

application = get_wsgi_application()
