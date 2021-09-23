# -*- coding: utf-8 -*-
"""syncmeta

Ad-hoc command to sync lab metadata and lims sheets to target db. Mainly for local dev purpose.

Usage:
    aws sso login --profile dev && export AWS_PROFILE=dev
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help syncmeta
    python manage.py syncmeta
    python manage.py syncmeta --lab
"""
from django.core.management import BaseCommand, CommandParser

from data_processors.lims.lambdas import labmetadata, google_lims


class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('--lims', help="Sync LIMS sheet only", action="store_true")
        parser.add_argument('--lab', help="Sync Lab Metadata sheet only", action="store_true")

    def handle(self, *args, **options):
        opt_lims = options['lims']
        opt_lab = options['lab']

        event = {
            'event': "Command event syncmeta",
        }

        if opt_lab:
            print(labmetadata.scheduled_update_handler(event, None))

        if opt_lims:
            print(google_lims.scheduled_update_handler(event, None))

        if not opt_lims and not opt_lab:
            print(labmetadata.scheduled_update_handler(event, None))
            print(google_lims.scheduled_update_handler(event, None))
