import logging
import re
import sys
# import uuid
from typing import Tuple

# from aws_xray_sdk.core import xray_recorder
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.db.models import QuerySet

from data_portal.models import Report, ReportType, S3Object
from data_processors.const import ReportHelper
from utils import libs3, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _extract_report_unique_key(key: str) -> Tuple:
    """
    Matches our special sauce key sequencing identifiers @UMCCR, i.e:
        SBJ66666__SBJ66666_MDX888888_L9999999_rerun-qc_summary
        SBJ00001__SBJ00001_PRJ000001_L0000001-hrdetect

    :param key: S3 key string
    :return: subject_id, sample_id, library_id Or None
    """
    # subsegment = xray_recorder.current_subsegment()

    subject_id_pattern = 'SBJ\d{5}'
    sample_id_pattern = '(?:PRJ|CCR|MDX)\d{6}'
    library_id_int_pattern = 'L\d{7}'
    library_id_ext_pattern = 'L' + sample_id_pattern
    library_id_extension_pattern = '(?:_topup\d?|_rerun\d?)'
    library_id_pattern = '(?:' + library_id_int_pattern + '|' + library_id_ext_pattern + ')' + library_id_extension_pattern + '?'

    # regex_key = re.compile('.+.umccrised.' + subject_id_pattern + '__(' + subject_id_pattern + ')_(' + sample_id_pattern + ')_(' + library_id_pattern +').+')
    # let's relax "umccrised" keyword for now
    regex_key = re.compile('.+.' + subject_id_pattern + '__(' + subject_id_pattern + ')_(' + sample_id_pattern + ')_(' + library_id_pattern + ').+')

    subject_id = None
    sample_id = None
    library_id = None

    match = regex_key.fullmatch(key)

    if match:
        match_groups = match.groups()
        subject_id = match_groups[0]
        sample_id = match_groups[1]
        library_id = match_groups[2]
    else:
        msg = f"Unable to extract report unique key. Unexpected pattern found: {key}"
        logger.warning(msg)
        # subsegment.put_metadata(f"WARN__{str(uuid.uuid4())}", {
        #     'key': key,
        #     'message': msg,
        # }, 'extract_report_unique_key')

    return subject_id, sample_id, library_id


def _extract_report_type(key: str):
    """
    Extract well-known Report type from key

    :param key:
    :return: report type Or None
    """
    # subsegment = xray_recorder.current_subsegment()

    # normalize
    key = key.lower()

    if "hrd/" in key:
        if "-chord." in key:
            return ReportType.HRD_CHORD
        elif "-hrdetect." in key:
            return ReportType.HRD_HRDETECT

    if "purple/" in key:
        if "_cnv_germ." in key:
            return ReportType.PURPLE_CNV_GERM
        elif "_cnv_som." in key:
            return ReportType.PURPLE_CNV_SOM
        elif "_cnv_som_gene." in key:
            return ReportType.PURPLE_CNV_SOM_GENE

    if "sigs/" in key:
        if "-dbs." in key:
            return ReportType.SIGS_DBS
        elif "-indel." in key:
            return ReportType.SIGS_INDEL
        elif "-snv_2015." in key:
            return ReportType.SIGS_SNV_2015
        elif "-snv_2020." in key:
            return ReportType.SIGS_SNV_2020

    if "sv/" in key:
        if "_unmelted." in key:
            return ReportType.SV_UNMELTED
        elif "_melted." in key:
            return ReportType.SV_MELTED
        elif "_bnd_main." in key:
            return ReportType.SV_BND_MAIN
        elif "_bnd_purpleinf." in key:
            return ReportType.SV_BND_PURPLEINF
        elif "_nobnd_main." in key:
            return ReportType.SV_NOBND_MAIN
        elif "_nobnd_other." in key:
            return ReportType.SV_NOBND_OTHER
        elif "_nobnd_manygenes." in key:
            return ReportType.SV_NOBND_MANYGENES
        elif "_nobnd_manytranscripts." in key:
            return ReportType.SV_NOBND_MANYTRANSCRIPTS

    if "-qc_summary." in key:
        return ReportType.QC_SUMMARY

    if "multiqc_data.json" in key:
        return ReportType.MULTIQC

    if "-report_inputs." in key:
        return ReportType.REPORT_INPUTS

    msg = f"Unknown report type. Unexpected pattern found: {key}"
    logger.warning(msg)
    # subsegment.put_metadata(f"WARN__{str(uuid.uuid4())}", {
    #     'key': key,
    #     'message': msg,
    # }, 'extract_report_type')
    return None


def parse_report_data(bucket, key):
    """
    Read report from bucket and parse its JSON content into Python dict

    :param bucket:
    :param key:
    :return:
    """
    # subsegment = xray_recorder.current_subsegment()

    if ReportHelper.extract_format(key) is None:
        logger.warning(f"Unsupported report format. Skip parsing report data: s3://{bucket}/{key}")
        return None

    decompressed_report: bytes = libs3.get_s3_object_to_bytes(bucket, key)

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
              f"Capturing report metadata only. Skip ingesting report data: s3://{bucket}/{key}"
        logger.warning(msg)
        # subsegment.put_metadata(f"WARN__{str(uuid.uuid4())}", {
        #     'key': key,
        #     'message': msg,
        # }, 'parse_report_data')
        data = None
    else:
        data = libjson.loads(decompressed_report)

    return data


@transaction.atomic
def persist_report(bucket: str, key: str, event_type):
    """
    Depends on event type, persist Report into db. Remove otherwise.

    :param bucket:
    :param key:
    :param event_type:
    """

    if not ReportHelper.is_report(key):
        logger.warning(f"Unrecognised report format or reporting source. Skip persisting report: s3://{bucket}/{key}")
        return None

    subject_id, sample_id, library_id = _extract_report_unique_key(key)
    if subject_id is None or sample_id is None or library_id is None:
        return None

    report_type = _extract_report_type(key)
    if report_type is None:
        return None

    if event_type == libs3.S3EventType.EVENT_OBJECT_CREATED.value:
        return _sync_report_created(bucket, key, subject_id, sample_id, library_id, report_type)
    elif event_type == libs3.S3EventType.EVENT_OBJECT_REMOVED.value:
        return _sync_report_deleted(key, subject_id, sample_id, library_id, report_type)


def _sync_report_created(bucket: str, key: str, subject_id: str, sample_id: str, library_id: str, report_type: str):
    # subsegment = xray_recorder.current_subsegment()

    qs: QuerySet = S3Object.objects.filter(bucket=bucket, key=key)

    data = parse_report_data(bucket, key)

    report = Report.objects.create_or_update_report(
        subject_id=subject_id,
        sample_id=sample_id,
        library_id=library_id,
        report_type=report_type,
        created_by=ReportHelper.extract_source(key),
        data=data,
        s3_object=qs.get() if qs.exists() else None
    )

    # subsegment.put_metadata(str(report.id.hex), {
    #     'key': key,
    #     'report': str(report),
    #     'size': str(decompressed_report_size_in_bytes),
    # }, 'sync_report_created')

    return report


def _sync_report_deleted(key: str, subject_id: str, sample_id: str, library_id: str, report_type: str):
    # subsegment = xray_recorder.current_subsegment()

    try:
        report: Report = Report.objects.get(
            subject_id=subject_id,
            sample_id=sample_id,
            library_id=library_id,
            type=report_type
        )
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
