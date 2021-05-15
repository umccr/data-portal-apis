# -*- coding: utf-8 -*-
"""module for data_processors package-level constants

Let's be Pythonic 💪 let's not mutate CAPITAL_VARIABLE elsewhere!
Consider Enum, if there's a need for un-mutable (name, value) and better protected tuple pair.
Or consider Helper class-ing where composite builder is needed.
"""
from abc import ABC

GDRIVE_SERVICE_ACCOUNT = "/umccr/google/drive/lims_service_account_json"
TRACKING_SHEET_ID = "/umccr/google/drive/tracking_sheet_id"
LIMS_SHEET_ID = "/umccr/google/drive/lims_sheet_id"


class ReportHelper(ABC):
    """Abstract helper for Report pipeline"""

    REPORT_EXTENSIONS = [
        "json.gz",
        "json",
    ]

    REPORT_KEYWORDS = [
        "cancer_report_tables",
        "multiqc_report_data",
        "TSO500_ctDNA",
    ]

    # Operational limit for decompressed report json data size ~10MB
    MAX_DECOMPRESSED_REPORT_SIZE_IN_BYTES = 11000000

    SQS_REPORT_EVENT_QUEUE_ARN = "/data_portal/backend/sqs_report_event_queue_arn"

    @classmethod
    def extract_format(cls, key: str):
        """
        We limit that all reports must be provided in JSON format.
        """

        key = key.lower()

        for ext in cls.REPORT_EXTENSIONS:
            if key.endswith(ext):
                return ext

        return None

    @classmethod
    def extract_source(cls, key: str):
        """
        Check S3 object key to determine report source.
        """

        key = key.lower()

        for keyword in cls.REPORT_KEYWORDS:
            if keyword in key:
                return keyword

        return None

    @classmethod
    def is_report(cls, key: str) -> bool:
        """
        Use report format and reporting source to determine further processing is required for report data ingestion.
        Filtering strategy is finding a very discriminated "keyword" in S3 object key and must be JSON format.
        """
        return True if cls.extract_format(key) is not None and cls.extract_source(key) is not None else False


class AbstractEventRecord(ABC):

    def __init__(self, event_type, event_time):
        self.event_type = event_type
        self.event_time = event_time


class S3EventRecord(AbstractEventRecord):
    """DTO class for S3 event message"""

    def __init__(self, event_type, event_time, s3_bucket_name, s3_object_meta):
        super().__init__(event_type, event_time)
        self.s3_bucket_name = s3_bucket_name
        self.s3_object_meta = s3_object_meta


class GDSEventRecord(AbstractEventRecord):
    """DTO class for GDS event message"""

    def __init__(self, event_type, event_time, gds_volume_name, gds_object_meta):
        super().__init__(event_type, event_time)
        self.gds_volume_name = gds_volume_name
        self.gds_object_meta = gds_object_meta
