import logging
import re
from typing import Tuple

from django.db.models import QuerySet

from data_portal.models import ReportType, GDSFile
from data_processors.const import ReportHelper
from data_processors.reports.services import SUBJECT_ID_PATTERN, SAMPLE_ID_PATTERN, LIBRARY_ID_PATTERN, \
    sync_report_created, sync_report_deleted, load_report_json
from utils import gds, ica

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _extract_report_unique_key(gds_path: str) -> Tuple:
    """
    Matches our CWL Workflow output arrangement in GDS

    Try with https://regex101.com
    .+.\/(SBJ\d{5})\/.+.\/((?:PRJ|CCR|MDX)\d{6})_((?:L\d{7}|L(?:PRJ|CCR|MDX)\d{6})(?:_topup\d?|_rerun\d?)?)\/.+

    :param gds_path: GDS File path string
    :return: subject_id, sample_id, library_id Or None
    """
    # subsegment = xray_recorder.current_subsegment()

    regex_gds_path = re.compile('.+./(' + SUBJECT_ID_PATTERN + ')/.+./(' + SAMPLE_ID_PATTERN + ')_(' + LIBRARY_ID_PATTERN + ')/.+')

    subject_id = None
    sample_id = None
    library_id = None

    match = regex_gds_path.fullmatch(gds_path)

    if match:
        match_groups = match.groups()
        subject_id = match_groups[0]
        sample_id = match_groups[1]
        library_id = match_groups[2]
    else:
        msg = f"Unable to extract report unique key. Unexpected pattern found: {gds_path}"
        logger.warning(msg)

        # subsegment.put_metadata(f"WARN__{str(uuid.uuid4())}", {
        #     'gds_path': gds_path,
        #     'message': msg,
        # }, 'extract_report_unique_key')

    return subject_id, sample_id, library_id


def _extract_report_type(gds_path: str):
    """
    Extract well-known Report type from gds path

    :param gds_path:
    :return: report type Or None
    """
    # subsegment = xray_recorder.current_subsegment()

    # normalize
    path_ = gds_path.lower()

    if "dragen_tso_ctdna/".lower() in path_:
        if ".msi.".lower() in path_:
            return ReportType.MSI
        elif ".tmb.".lower() in path_:
            return ReportType.TMB
        elif "_TMB_Trace.".lower() in path_:
            return ReportType.TMB_TRACE
        elif ".AlignCollapseFusionCaller_metrics.".lower() in path_:
            return ReportType.FUSION_CALLER_METRICS
        elif "_Failed_Exon_coverage_QC.".lower() in path_:
            return ReportType.FAILED_EXON_COVERAGE_QC
        elif "_SampleAnalysisResults.".lower() in path_:
            return ReportType.SAMPLE_ANALYSIS_RESULTS
        elif ".TargetRegionCoverage.".lower() in path_:
            return ReportType.TARGET_REGION_COVERAGE

    msg = f"Unknown report type. Unexpected pattern found: {gds_path}"
    logger.warning(msg)

    # subsegment.put_metadata(f"WARN__{str(uuid.uuid4())}", {
    #     'gds_path': gds_path,
    #     'message': msg,
    # }, 'extract_report_type')

    return None


def parse_report_data(gds_volume_name, gds_path):
    """
    Read report from gds_volume_name and parse its JSON content into Python dict

    :param gds_volume_name:
    :param gds_path:
    :return:
    """
    # subsegment = xray_recorder.current_subsegment()

    report_uri = gds.get_gds_uri(gds_volume_name, gds_path)

    if ReportHelper.extract_format(gds_path) is None:
        logger.warning(f"Unsupported report format. Skip parsing report data: {report_uri}")
        return None

    decompressed_report: bytes = gds.get_gds_file_to_bytes(gds_volume_name, gds_path)

    return load_report_json(decompressed_report, report_uri)


def persist_report(gds_volume_name, gds_path, event_type):
    """
    Depends on event type, persist Report into db. Remove otherwise.

    :param gds_volume_name:
    :param gds_path:
    :param event_type:
    """

    report_uri = gds.get_gds_uri(gds_volume_name, gds_path)

    if not ReportHelper.is_report(gds_path):
        logger.warning(f"Unrecognised report format or reporting source. Skip persisting report: {report_uri}")
        return None

    subject_id, sample_id, library_id = _extract_report_unique_key(gds_path)
    if subject_id is None or sample_id is None or library_id is None:
        return None

    report_type = _extract_report_type(gds_path)
    if report_type is None:
        return None

    payload = {
        'subject_id': subject_id,
        'sample_id': sample_id,
        'library_id': library_id,
        'report_type': report_type,
        'report_uri': report_uri,
        'created_by': ReportHelper.extract_source(gds_path),
    }

    create_or_update_events = [
        ica.GDSFilesEventType.UPLOADED.value,
        ica.GDSFilesEventType.ARCHIVED.value,
        ica.GDSFilesEventType.UNARCHIVED.value,
    ]

    if event_type in create_or_update_events:
        data = parse_report_data(gds_volume_name, gds_path)
        qs: QuerySet = GDSFile.objects.filter(volume_name=gds_volume_name, path=gds_path)

        payload.update(data=data)
        payload.update(gds_file=qs.get() if qs.exists() else None)

        return sync_report_created(payload)

    elif event_type == ica.GDSFilesEventType.DELETED.value:
        return sync_report_deleted(payload)
