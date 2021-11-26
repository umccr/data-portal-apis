# -*- coding: utf-8 -*-
"""addreport

Add report from S3 URI location. This ad-hoc command is mainly for local dev purpose.

Usage:
    aws sso login --profile dev && export AWS_PROFILE=dev
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help addreport
    time python manage.py addreport s3://my_bucket/path/to/file.json.gz
    time python manage.py addreport gds://development/analysis_data/SBJ00476/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ200603_L2100345/PRJ200603_L2100345.AlignCollapseFusionCaller_metrics.json.gz
"""
import logging
import sys
from datetime import datetime

from django.core.management import BaseCommand, CommandParser, execute_from_command_line
from django.utils.timezone import make_aware
from libica.app import GDSFilesEventType
from libumccr.aws import libs3

from data_processors.reports.lambdas import report_event


def set_db_debug():
    """log raw sql query to console"""
    logger = logging.getLogger('django.db.backends')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))


class Command(BaseCommand):

    def __init__(self):
        super().__init__()
        self.opt_path = None

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('path', help="GDS or S3 URI")

    def handle(self, *args, **options):
        self.opt_path = options['path']

        # set_db_debug()

        if str(self.opt_path).startswith('s3://'):
            self.perform_s3()

        elif str(self.opt_path).startswith('gds://'):
            self.perform_gds()

        else:
            execute_from_command_line(['./manage.py', 'help', 'addreport'])
            exit(0)

    def perform_s3(self):
        base_path = str(self.opt_path).split('s3://')[1]
        bucket = base_path.split('/')[0]
        key = base_path.lstrip(bucket).lstrip('/')

        if bucket is None or key is None:
            execute_from_command_line(['./manage.py', 'help', 'addreport'])
            exit(0)

        exist, resp = libs3.head_s3_object(bucket, key)

        if exist:
            # print(resp)

            report_event.handler({
                "event_type": libs3.S3EventType.EVENT_OBJECT_CREATED.value,
                "event_time": make_aware(datetime.now()),
                "s3_bucket_name": bucket,
                "s3_object_meta": {
                    "versionId": resp['VersionId'],
                    "size": resp['ContentLength'],
                    "eTag": resp['ETag'],
                    "key": key,
                    "sequencer": ""
                }
            }, None)

        else:
            print(f"Object does not exist. {resp['error']}")

    def perform_gds(self):
        base_path = str(self.opt_path).split('gds://')[1]
        volume_name = base_path.split('/')[0]
        path_ = base_path.lstrip(volume_name)

        if volume_name is None or path_ is None:
            execute_from_command_line(['./manage.py', 'help', 'addreport'])
            exit(0)

        report_event.handler({
            "event_type": GDSFilesEventType.UPLOADED.value,
            "event_time": make_aware(datetime.now()),
            "gds_volume_name": volume_name,
            "gds_object_meta": {
                "path": path_,
                "volumeName": volume_name,
            }
        }, None)
