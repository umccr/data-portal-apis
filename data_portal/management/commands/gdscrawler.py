# -*- coding: utf-8 -*-
"""gdscrawler

Meant to run as offline tool to ingest GDS files metadata into Portal database

Usage:
    aws sso login --profile dev && export AWS_PROFILE=dev
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help gdscrawler
    python manage.py gdscrawler umccr-temp-data-dev --dry --log
"""
import logging
from datetime import datetime

from django.core.management import BaseCommand, CommandParser
from libica.openapi import libgds

from data_processors.gds import services
from utils import ica, libdt

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('volume', help="GDS volume name")
        parser.add_argument('-d', '--dry', help="Dry run", action="store_true")
        parser.add_argument('-l', '--log', help="Output to log file", action="store_true")

    def handle(self, *args, **options):
        opt_volume = options['volume']
        opt_dry = options['dry']
        opt_log = options['log']

        if opt_log:
            log_file = logging.FileHandler("gdscrawler-{}.log".format(datetime.now().strftime("%Y%m%d%H%M%S")))
            log_file.setLevel(logging.INFO)
            log_file.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
            logger.addHandler(log_file)

        gds_volume = opt_volume

        with libgds.ApiClient(ica.configuration(libgds)) as gds_client:
            files_api = libgds.FilesApi(gds_client)
            try:
                logger.info(f"Crawling files metadata from volume gds://{gds_volume}")

                uin = input("WARNING: this process may take time. Continue? (y or n): ")
                if uin != 'y':
                    logger.info("Abort upon user request")
                    exit(0)

                cnt = 0
                page_token = None
                volume_name = [f"{gds_volume}", ]
                output_path = ["/*", ]
                while True:
                    file_list: libgds.FileListResponse = files_api.list_files(
                        volume_name=volume_name,
                        path=output_path,
                        page_size=1000,
                        page_token=page_token,
                    )

                    for item in file_list.items:
                        file: libgds.FileResponse = item

                        # transform back into REST style representation
                        file_rest_repr = {}
                        for k, v in file.attribute_map.items():
                            item = getattr(file, k)
                            if isinstance(item, datetime):
                                item = libdt.serializable_datetime(item)
                            file_rest_repr[v] = item

                        if opt_dry:
                            logger.info(f"gds://{file.volume_name}{file.path}")
                        else:
                            services.create_or_update_gds_file(file_rest_repr)
                        cnt += 1

                    page_token = file_list.next_page_token
                    if not file_list.next_page_token:
                        break

                # end while
                logger.info(f"Total {cnt} files have been crawled from gds://{gds_volume}")

            except libgds.ApiException as e:
                logger.error(f"Exception - {e}")
