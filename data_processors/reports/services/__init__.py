import logging
import sys

from django.core.exceptions import ObjectDoesNotExist

from data_portal.models.report import Report
from data_processors.const import ReportHelper
from utils import libjson

logger = logging.getLogger()


SUBJECT_ID_PATTERN = 'SBJ\d{5}'
SAMPLE_ID_PATTERN = '(?:PRJ|CCR|MDX)\d{6}'
LIBRARY_ID_INT_PATTERN = 'L\d{7}'
LIBRARY_ID_EXT_PATTERN = 'L' + SAMPLE_ID_PATTERN
LIBRARY_ID_EXTENSION_PATTERN = '(?:_topup\d?|_rerun\d?)'
LIBRARY_ID_PATTERN = '(?:' + LIBRARY_ID_INT_PATTERN + '|' + LIBRARY_ID_EXT_PATTERN + ')' + LIBRARY_ID_EXTENSION_PATTERN + '?'


def load_report_json(decompressed_report, report_uri):
    decompressed_report_size_in_bytes: int = sys.getsizeof(decompressed_report)
    logger.info(f"Decompressed report size in bytes: {decompressed_report_size_in_bytes}")

    if decompressed_report_size_in_bytes > ReportHelper.MAX_DECOMPRESSED_REPORT_SIZE_IN_BYTES:
        """NOTE: Here, report data is too large if > 150MB
        We will just capture its metadata only and skip ingesting json data.
        Based on offline debug study, it is known to require:
            - RDS Aurora Serverless 8AU capacity (8*2GB ~16GB)
            - Lambda processing memory 4096MB (4GB)
            - 3x Lambda invocations 
                - First 2 invocations raised OperationalError: (2013, 'Lost connection to MySQL server during query')
                - That, in turns trigger scale up event for RDS Aurora Serverless cluster
                - And, 3rd invocation got through at RDS Aurora 8AU capacity 
            - Processing time elapse avg 44604.08 ms (~44s)
            - For ingesting decompressed report json data size of 167009928 bytes (~167.01MB)  
        """
        msg = f"Report too large. Decompressed size in bytes: {decompressed_report_size_in_bytes}. " \
              f"Capturing report metadata only. Skip ingesting report data: {report_uri}"
        logger.warning(msg)

        # subsegment.put_metadata(f"WARN__{str(uuid.uuid4())}", {
        #     'gds_path': gds_path,
        #     'message': msg,
        # }, 'parse_report_data')

        data = None
    else:
        data = libjson.loads(decompressed_report)

    return data


def sync_report_created(payload: dict):
    # subsegment = xray_recorder.current_subsegment()

    report = Report.objects.create_or_update_report(
        subject_id=payload['subject_id'],
        sample_id=payload['sample_id'],
        library_id=payload['library_id'],
        report_type=payload['report_type'],
        report_uri=payload['report_uri'],
        created_by=payload.get('created_by', None),
        data=payload.get('data', None),
        s3_object=payload.get('s3_object', None),
        gds_file=payload.get('gds_file', None),
    )

    # subsegment.put_metadata(str(report.id.hex), {
    #     'key': key,
    #     'report': str(report),
    #     'size': str(decompressed_report_size_in_bytes),
    # }, 'sync_report_created')

    return report


def sync_report_deleted(payload: dict):
    # subsegment = xray_recorder.current_subsegment()

    subject_id = payload['subject_id']
    sample_id = payload['sample_id']
    library_id = payload['library_id']
    report_type = payload['report_type']
    report_uri = payload['report_uri']

    try:
        report: Report = Report.objects.get_by_unique_fields(
            subject_id=subject_id,
            sample_id=sample_id,
            library_id=library_id,
            report_type=report_type,
            report_uri=report_uri,
        ).get()

        # subsegment.put_metadata(str(report.id.hex), {
        #     'key': key,
        #     'report': str(report),
        # }, 'sync_report_deleted')

        report.delete()
        return report

    except ObjectDoesNotExist as e:
        logger.info(f"No deletion required. Non-existent Report (subject_id={subject_id}, sample_id={sample_id}, "
                    f"library_id={library_id}, type={report_type}) : {str(e)}")
        return None
