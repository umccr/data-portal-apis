# -*- coding: utf-8 -*-
"""s3crawler

Meant to run one time, offline tool to index S3 objects.
Typically, we run this on EC2 instance that has access to RDS database in Private Subnet.
Otherwise, locally as an example shown below.

Usage:
    aws sso login --profile dev && export AWS_PROFILE=dev
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help s3crawler
    python manage.py s3crawler umccr-temp-dev --dry --log
    python manage.py s3crawler umccr-temp-dev --key some/folder/ --dry --log
"""
import logging
from datetime import datetime

from django.core.management import BaseCommand, CommandParser
from libumccr.aws import libs3

from data_processors.s3 import services

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('bucket', help="S3 bucket name")
        parser.add_argument('-k', '--key', help="S3 key prefix")
        parser.add_argument('-d', '--dry', help="Dry run", action="store_true")
        parser.add_argument('-l', '--log', help="Output to log file", action="store_true")

    def handle(self, *args, **options):
        opt_bucket = options['bucket']
        opt_key = options['key']
        opt_dry = options['dry']
        opt_log = options['log']

        if opt_log:
            log_file = logging.FileHandler("s3crawler-{}.log".format(datetime.now().strftime("%Y%m%d%H%M%S")))
            log_file.setLevel(logging.INFO)
            log_file.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
            logger.addHandler(log_file)

        bucket = opt_bucket
        key_prefix = opt_key if opt_key else ""

        logger.info(f"Indexing S3 objects from bucket ({bucket}) with prefix ({key_prefix})")

        uin = input("WARNING: this process may take time and API request cost. Continue? (y or n): ")

        if uin == 'y':
            cnt = 0
            batch_list = []
            for obj in libs3.get_matching_s3_objects(bucket, prefix=key_prefix):
                if opt_dry:
                    logger.info(f"s3://{bucket}/{obj['Key']}")
                else:
                    batch_list.append(
                        services.persist_s3_object(
                            bucket=bucket,
                            key=obj['Key'],
                            last_modified_date=obj['LastModified'],
                            e_tag=str(obj['ETag'][1:-1]),
                            size=int(obj['Size']),
                        )
                    )

                    if len(batch_list) == 10:
                        services.persist_s3_object_bulk(batch_list)
                        batch_list.clear()

                cnt += 1

            # any remainder in the last batch
            services.persist_s3_object_bulk(batch_list)

            logger.info(f"Total {cnt} objects have been indexd from s3://{bucket}/{key_prefix}")
        else:
            logger.info("Abort upon user request")
