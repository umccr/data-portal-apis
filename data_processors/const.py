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
