import logging
import uuid

from django.db import models
from django.db.models import QuerySet

from data_portal.fields import HashField, HashFieldHelper
from data_portal.models.gdsfile import GDSFile
from data_portal.models.s3object import S3Object
from django.core.exceptions import FieldError

from .utils import filter_object_by_parameter_keyword

logger = logging.getLogger(__name__)


class ReportType(models.TextChoices):
    MSI = "msi"
    TMB = "tmb"
    TMB_TRACE = "tmb_trace"
    FUSION_CALLER_METRICS = "fusion_caller_metrics"
    FAILED_EXON_COVERAGE_QC = "failed_exon_coverage_qc"
    SAMPLE_ANALYSIS_RESULTS = "sample_analysis_results"
    TARGET_REGION_COVERAGE = "target_region_coverage"
    QC_SUMMARY = "qc_summary"
    MULTIQC = "multiqc"
    REPORT_INPUTS = "report_inputs"
    HRD_CHORD = "hrd_chord"
    HRD_HRDETECT = "hrd_hrdetect"
    PURPLE_CNV_GERM = "purple_cnv_germ"
    PURPLE_CNV_SOM = "purple_cnv_som"
    PURPLE_CNV_SOM_GENE = "purple_cnv_som_gene"
    SIGS_DBS = "sigs_dbs"
    SIGS_INDEL = "sigs_indel"
    SIGS_SNV_2015 = "sigs_snv_2015"
    SIGS_SNV_2020 = "sigs_snv_2020"
    SV_UNMELTED = "sv_unmelted"
    SV_MELTED = "sv_melted"
    SV_BND_MAIN = "sv_bnd_main"
    SV_BND_PURPLEINF = "sv_bnd_purpleinf"
    SV_NOBND_MAIN = "sv_nobnd_main"
    SV_NOBND_OTHER = "sv_nobnd_other"
    SV_NOBND_MANYGENES = "sv_nobnd_manygenes"
    SV_NOBND_MANYTRANSCRIPTS = "sv_nobnd_manytranscripts"


class ReportManager(models.Manager):

    def get_by_unique_fields(
            self,
            subject_id: str,
            sample_id: str,
            library_id: str,
            report_type: str,
            report_uri: str,
    ) -> QuerySet:

        h = HashFieldHelper()
        h.add(subject_id).add(sample_id).add(library_id).add(report_type).add(report_uri)

        return self.filter(unique_hash__exact=h.calculate_hash())

    def create_or_update_report(
            self,
            subject_id: str,
            sample_id: str,
            library_id: str,
            report_type: str,
            created_by: str,
            data,
            s3_object: S3Object,
            gds_file: GDSFile,
            report_uri: str,
    ):
        qs: QuerySet = self.get_by_unique_fields(
            subject_id=subject_id,
            sample_id=sample_id,
            library_id=library_id,
            report_type=report_type,
            report_uri=report_uri,
        )

        if qs.exists():
            # update existing report
            report = qs.get()
        else:
            # new report
            report = Report()
            report.subject_id = subject_id
            report.sample_id = sample_id
            report.library_id = library_id
            report.type = report_type
            report.report_uri = report_uri

        report.created_by = created_by
        report.data = data
        report.s3_object_id = None if s3_object is None else s3_object.id
        report.gds_file_id = None if gds_file is None else gds_file.id
        report.save()
        return report

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        OBJECT_FIELD_NAMES = self.values()[0].keys()

        keywords = kwargs.get('keywords', None)
        if keywords:
            try:
                qs = filter_object_by_parameter_keyword(qs, keywords, OBJECT_FIELD_NAMES)
            except FieldError:
                qs = self.none()

        return qs


class Report(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    subject_id = models.CharField(max_length=255)
    sample_id = models.CharField(max_length=255)
    library_id = models.CharField(max_length=255)
    type = models.CharField(choices=ReportType.choices, max_length=255)
    created_by = models.CharField(max_length=255, null=True, blank=True)
    data = models.JSONField(null=True, blank=True)  # note max 1GB in size for a json document

    s3_object_id = models.BigIntegerField(null=True, blank=True)
    gds_file_id = models.BigIntegerField(null=True, blank=True)

    report_uri = models.TextField(default='None')

    unique_hash = HashField(unique=True, base_fields=[
        'subject_id', 'sample_id', 'library_id', 'type', 'report_uri'
    ], null=True, default=None)

    objects = ReportManager()

    def __str__(self):
        return f"ID: {self.id}, SUBJECT_ID: {self.subject_id}, SAMPLE_ID: {self.sample_id}, " \
               f"LIBRARY_ID: {self.library_id}, TYPE: {self.type}"
