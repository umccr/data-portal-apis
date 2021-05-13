# -*- coding: utf-8 -*-
"""tagger

Meant to run one time, offline tool to re/process S3 tagging

Cost:
1. Listing bucket is paginated 1000 objects per request, see libs3.get_matching_s3_keys(..), therefore
    450,000 objects at 1000 objects per request = 450 LIST requests
    450 requests at $0.0055 per 1000 requests = $0.002475
2. Tagging an object requires 1 GET request and 1 PUT request, therefore, say 2712 .bam out of 450,000
    1 PUT + 1 GET request at $0.0055 and $0.00044 per 1000 requests = $0.00594 per 1000 requests
    For 2712 .bam objects, 2712 * 0.00594 / 1000 = $0.01610928

Usage:
    aws sso login --profile dev && export AWS_PROFILE=dev
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help tagger
    python manage.py tagger umccr-primary-data-dev --dry --log
"""
import logging
from datetime import datetime

from django.core.management import BaseCommand, CommandParser

from data_processors.s3 import services
from utils import libs3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FILTER_SUFFIX = ".bam"


class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('bucket', help="S3 bucket name")
        parser.add_argument('-d', '--dry', help="Dry run", action="store_true")
        parser.add_argument('-l', '--log', help="Output to log file", action="store_true")

    def handle(self, *args, **options):
        opt_bucket = options['bucket']
        opt_dry = options['dry']
        opt_log = options['log']

        if opt_log:
            log_file = logging.FileHandler("tagger-{}.log".format(datetime.now().strftime("%Y%m%d%H%M%S")))
            log_file.setLevel(logging.INFO)
            log_file.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
            logger.addHandler(log_file)

        bucket = opt_bucket

        logger.info(f"Tagging S3 objects from bucket ({bucket}) with extension filter ({FILTER_SUFFIX})")

        uin = input("WARNING: this process may take time and API request cost. Continue? (y or n): ")

        if uin == 'y':
            cnt = 0
            for key in libs3.get_matching_s3_keys(bucket, suffix=FILTER_SUFFIX):
                logger.info(f"s3://{bucket}/{key}") if opt_dry else services.tag_s3_object(bucket, key, "bam")
                cnt += 1
            logger.info(f"Total {cnt} objects have been tagged from s3://{bucket}")
        else:
            logger.info("Abort upon user request")
