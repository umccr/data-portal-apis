import django
import os

# Need to set up django app first
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_portal.settings')
django.setup()

# ---

import logging
import sys
from datetime import datetime

from data_processors import services
from utils import libs3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_file = logging.FileHandler("tagger-{}.log".format(datetime.now().strftime("%Y%m%d%H%M%S")))
log_file.setLevel(logging.INFO)
log_file.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
logger.addHandler(log_file)


FILTER_SUFFIX = ".bam"

if __name__ == '__main__':
    """
    Meant to run one time, offline tool to re/process tagging as implemented in services.tag_s3_object(..)
    
    Cost:
    1. Listing bucket is paginated 1000 objects per request, see libs3.get_matching_s3_keys(..), therefore
        450,000 objects at 1000 objects per request = 450 LIST requests
        450 requests at $0.0055 per 1000 requests = $0.002475
    2. Tagging an object requires 1 GET request and 1 PUT request, therefore, say 2712 .bam out of 450,000 
        1 PUT + 1 GET request at $0.0055 and $0.00044 per 1000 requests = $0.00594 per 1000 requests
        For 2712 .bam objects, 2712 * 0.00594 / 1000 = $0.01610928    
    
    Usage:
    1. For Django settings, terraform output and export all CAPITAL variables as environment variables
    2. ssoawsprod and export AWS_PROFILE=prod
    3. screen or tmux session
    4. python tagger.py bucket-name-prod
    """

    args = sys.argv[1:]
    if len(args) != 1:
        print("Please provide bucket name. Usage: \n\t python tagger.py my-bucket-name")
        exit(1)

    bucket = args[0]

    if not libs3.bucket_exists(bucket):
        exit(1)

    logger.info(f"Tagging S3 objects from bucket ({bucket}) with extension filter ({FILTER_SUFFIX})")

    uin = input("WARNING: this process may take time and API request cost. Continue? (y or n): ")

    if uin == 'y':
        for key in libs3.get_matching_s3_keys(bucket, suffix=FILTER_SUFFIX):
            services.tag_s3_object(bucket, key)
    else:
        logger.info("Abort upon user request")
