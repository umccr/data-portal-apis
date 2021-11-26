import logging
import re
# import uuid
from typing import Tuple

# from aws_xray_sdk.core import xray_recorder
from django.db import transaction
from django.db.models import QuerySet
from libumccr.aws import libs3

from data_portal.models.report import ReportType
from data_portal.models.s3object import S3Object
from data_processors.const import ReportHelper
from data_processors.reports.services import SUBJECT_ID_PATTERN, SAMPLE_ID_PATTERN, LIBRARY_ID_PATTERN, \
    sync_report_created, sync_report_deleted, load_report_json

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

    regex_key = re.compile('.+.' + SUBJECT_ID_PATTERN + '__(' + SUBJECT_ID_PATTERN + ')_(' + SAMPLE_ID_PATTERN + ')_(' + LIBRARY_ID_PATTERN + ').+')

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

    report_uri = libs3.get_s3_uri(bucket, key)

    if ReportHelper.extract_format(key) is None:
        logger.warning(f"Unsupported report format. Skip parsing report data: {report_uri}")
        return None

    decompressed_report: bytes = libs3.get_s3_object_to_bytes(bucket, key)

    return load_report_json(decompressed_report, report_uri)


@transaction.atomic
def persist_report(bucket: str, key: str, event_type):
    """
    Depends on event type, persist Report into db. Remove otherwise.

    :param bucket:
    :param key:
    :param event_type:
    """

    report_uri = libs3.get_s3_uri(bucket, key)

    if not ReportHelper.is_report(key):
        logger.warning(f"Unrecognised report format or reporting source. Skip persisting report: {report_uri}")
        return None

    subject_id, sample_id, library_id = _extract_report_unique_key(key)
    if subject_id is None or sample_id is None or library_id is None:
        return None

    report_type = _extract_report_type(key)
    if report_type is None:
        return None

    payload = {
        'subject_id': subject_id,
        'sample_id': sample_id,
        'library_id': library_id,
        'report_type': report_type,
        'report_uri': report_uri,
        'created_by': ReportHelper.extract_source(key),
    }

    if event_type == libs3.S3EventType.EVENT_OBJECT_CREATED.value:
        data = parse_report_data(bucket, key)
        qs: QuerySet = S3Object.objects.filter(bucket=bucket, key=key)

        payload.update(data=data)
        payload.update(s3_object=qs.get() if qs.exists() else None)

        return sync_report_created(payload)

    elif event_type == libs3.S3EventType.EVENT_OBJECT_REMOVED.value:
        return sync_report_deleted(payload)
