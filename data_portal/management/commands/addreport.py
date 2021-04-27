import logging
import sys
from datetime import datetime

from django.core.management import BaseCommand, CommandParser, execute_from_command_line
from django.utils.timezone import make_aware

from data_processors.reports.lambdas import report_event
from data_processors.s3.helper import S3EventType
from utils import libs3


def set_db_debug():
    """log raw sql query to console"""
    logger = logging.getLogger('django.db.backends')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))


class Command(BaseCommand):
    """
    Add report from S3 URI location. This ad-hoc command is mainly for local dev purpose.
    Usage:
        aws sso login --profile dev && export AWS_PROFILE=dev
        export DJANGO_SETTINGS_MODULE=data_portal.settings.local
        python manage.py migrate
        python manage.py help addreport
        time python manage.py addreport s3://my_bucket/path/to/file.json.gz
    """
    def add_arguments(self, parser: CommandParser):
        parser.add_argument('path', help="S3 URI e.g. s3://my_bucket/path/to/report_file.json.gz")

    def handle(self, *args, **options):
        opt_path = options['path']

        # set_db_debug()

        if not str(opt_path).startswith('s3://'):
            execute_from_command_line(['./manage.py', 'help', 'addreport'])
            exit(0)

        base_path = str(opt_path).split('s3://')[1]
        bucket = base_path.split('/')[0]
        key = base_path.lstrip(bucket).lstrip('/')

        if bucket is None or key is None:
            execute_from_command_line(['./manage.py', 'help', 'addreport'])
            exit(0)

        exist, resp = libs3.head_s3_object(bucket, key)

        if exist:
            # print(resp)

            report_event.handler({
                "event_type": S3EventType.EVENT_OBJECT_CREATED.value,
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
