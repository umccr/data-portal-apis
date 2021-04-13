import re
import logging
from typing import List, Tuple

from data_portal.models import Report
from data_processors.s3.helper import S3EventRecord
from utils import libs3, libjson

from aws_xray_sdk.core import xray_recorder

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _extract_report_unique_key(key) -> Tuple:
    """
    Matches our special sauce key sequencing identifiers @UMCCR, i.e:
    "SBJ66666__SBJ66666_MDX888888_L9999999_rerun-qc_summary.json.gz"
    :param key: S3 key string
    :return: subject_id, sample_id and library_id strings
    """
    subject_id_pattern = 'SBJ\d{5}'
    sample_id_pattern = '(?:PRJ|CCR|MDX)\d{6}'
    library_id_int_pattern = 'L\d{7}'
    library_id_ext_pattern = 'L' + sample_id_pattern
    library_id_extension_pattern = '(?:_topup\d?|_rerun\d?)'
    library_id_pattern = '(?:' + library_id_int_pattern + '|' + library_id_ext_pattern + ')' + library_id_extension_pattern + '?'

    regex_key = re.compile('.+.umccrised.' + subject_id_pattern + '__(' + subject_id_pattern + ')_(' + sample_id_pattern + ')_(' + library_id_pattern +').+')

    match = regex_key.fullmatch(key)

    if match:
        match_groups = match.groups()
        subject_id = match_groups[0]
        sample_id = match_groups[1]
        library_id = match_groups[2]

    return subject_id, sample_id, library_id


def serialize_to_cancer_report(records: List[S3EventRecord]) -> bool:
    """
    Filters and distributes particular S3 objects for further processing.

    s3://clinical-patient-data-bucket/['subject_id', 'sample_id', 'library_id']/cancer_report_tables/json/{hrd|purple|sigs|sv}

    :param records: S3 events to be processed coming from the SQS queue
    :return: The serialization of the JSON records was successfully imported into the ORM (or not)
    """
    subsegment = xray_recorder.begin_subsegment('serialize_cancer_report')

    for record in records:
        bucket = record.s3_bucket_name
        key = record.s3_object_meta['key']
        try:
            if 'cancer_report_tables' in key:
                if 'json.gz' in key:
                    # Fetches S3 object and decompresses/deserializes it
                    obj_bytes = libs3.get_s3_object_to_bytes(bucket, key)
                    json_dict = libjson.loads(obj_bytes)
                    json_dict['subject_id'], json_dict['sample_id'], json_dict['library_id'] = _extract_report_unique_key(key)

                    subsegment.put_metadata('json_dict_cancer_report', json_dict, 'cancer_report')
                    xray_recorder.end_subsegment()
                    # Adds attributes from JSON to Django Report model
                    json_report = libjson.dumps(json_dict)
                    report: Report = Report.objects.put(json_report)
            return True
        except:
            return False
