import logging
import json

from data_portal.models import Report
from utils import libs3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def persist_sequencing_report(bucket: str, key: str, payload: object):
    # TODO:
    #  1. Check input sizes and adjust lambda runtimes?
    query_set = Report.objects.filter(bucket=bucket, key=key)
    new = not query_set.exists()

    if new:
        logger.info(f"Creating a new Report key={key}")
        report = Report(
            bucket=bucket,
            key=key
        )
    else:
        logger.info(f"Updating a existing Report (bucket={bucket}, key={key})")
        report: Report = query_set.get()

    # TODO: Cleanup/validate json attribute slurping
    report_json = json.loads(libs3.get_s3_object_to_bytes(bucket=bucket, key=key))
    report.save(report_json)

def process_sequencing_report(bucket: str, key: str):
    if key.endswith('.json.gz'):
        # TODO: Make sure we have a stable naming to detect those S3 report events properly
        if "reports" in key:
            payload = libs3.get_s3_object(bucket=bucket, key=key)
            if payload['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.info(f"Found sequencing report, importing into database: {key}")
                persist_sequencing_report(bucket, key, payload)
            else:
                logger.error(f"Failed to retrieve sequecing report: {key}")
