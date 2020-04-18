# -*- coding: utf-8 -*-
"""gdscrawler app

Meant to run as offline tool to ingest GDS files metadata into Portal database

Usage:
0. export IAP_BASE_URL=<baseUrl>  e.g. https://aps2.plaftorm.illumina.com
1. export IAP_AUTH_TOKEN=<tok>
2. export IAP_GDS_VOLUME=<volNameOrId>  e.g. umccr-temp-dev  Optional otherwise, see --volume
3. For Django settings, terraform output and export all CAPITAL variables as environment variables
4. ssoawsprod and export AWS_PROFILE=prod
5. screen or tmux session
6. python -m data_processors.scripts.gdscrawler -h
7. python -m data_processors.scripts.gdscrawler -v umccr-primary-data-dev -l
"""
import argparse
import json
import logging
import os
from datetime import datetime

from utils import libgds

logger = logging.getLogger()
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    import django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
    django.setup()
    from data_processors import services

    # ---

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--volume", help="GDS volume name or ID")
    parser.add_argument("-d", "--dry", help="Dry run -- log instead", action="store_true")
    parser.add_argument("-l", "--log", help="Output to log file", action="store_true")
    args = parser.parse_args()

    if args.log:
        log_file = logging.FileHandler("gdscrawler-{}.log".format(datetime.now().strftime("%Y%m%d%H%M%S")))
        log_file.setLevel(logging.INFO)
        log_file.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
        logger.addHandler(log_file)

    iap_gds_volume = args.volume

    if iap_gds_volume is None:
        logger.info(f"Option --volume is not specified. Looking IAP_GDS_VOLUME from env.")
        iap_gds_volume = os.getenv('IAP_GDS_VOLUME', None)

    assert iap_gds_volume is not None, "IAP_GDS_VOLUME is not defined"

    logger.info(f"Getting info on volume: gds://{iap_gds_volume}")
    volume = libgds.get_volume(volume_id=iap_gds_volume)
    logger.debug(json.dumps(volume))

    logger.info(f"Crawling files metadata from volume gds://{volume['name']}")

    uin = input("WARNING: this process may take time. Continue? (y or n): ")

    if uin == 'y':
        cnt = 0
        for file in libgds.list_files(volume_name=volume['name']):
            if args.dry:
                logger.info(f"gds://{file['volumeName']}{file['path']}")
            else:
                services.create_or_update_gds_file(file)
            cnt += 1
        logger.info(f"Total {cnt} files have been crawled from gds://{volume['name']}")
    else:
        logger.info("Abort upon user request")
