def handler(event, context):
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django.settings")
    django.setup()