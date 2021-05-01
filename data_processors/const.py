# -*- coding: utf-8 -*-
"""module for data_processors package-level constants

Let's be Pythonic 💪 let's not mutate CAPITAL_VARIABLE elsewhere!
Consider Enum, if there's a need for un-mutable (name, value) and better protected tuple pair.
Or consider Helper class-ing where composite builder is needed.
"""

GDRIVE_SERVICE_ACCOUNT = "/umccr/google/drive/lims_service_account_json"
TRACKING_SHEET_ID = "/umccr/google/drive/tracking_sheet_id"
LIMS_SHEET_ID = "/umccr/google/drive/lims_sheet_id"

JSON_GZ = "json.gz"
CANCER_REPORT_TABLES = "cancer_report_tables"

# Operational limit for decompressed report json data size ~10MB
MAX_DECOMPRESSED_REPORT_SIZE_IN_BYTES = 11000000
