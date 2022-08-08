import logging

from django.db import models
from django.db.models import QuerySet, Q

from data_portal.fields import HashField

logger = logging.getLogger(__name__)


class GDSFileManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.exclude(path__contains='.snakemake')

        volume_name = kwargs.get('volume_name', None)
        if volume_name:
            qs = qs.filter(volume_name=volume_name)

        subject = kwargs.get('subject', None)
        if subject:
            qs = qs.filter(path__icontains=subject)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(path__icontains=run)

        return qs

    def get_subject_results(self, subject_id: str, **kwargs):
        qs: QuerySet = self.filter(path__icontains=subject_id)

        bam = Q(path__iregex='wgs') & Q(path__iregex='tumor') & Q(path__iregex='normal') & Q(path__iregex='.bam$')
        vcf = (Q(path__iregex='umccrise/[^\/]*/[^\/]*/[^(work)*]')
               & Q(path__iregex='small_variants/[^\/]*(.vcf.gz$|.maf$)'))

        cancer = Q(path__iregex='umccrise') & Q(path__iregex='cancer_report.html$')
        qc = Q(path__iregex='umccrise') & Q(path__iregex='multiqc_report.html$')
        pcgr = Q(path__iregex='umccrise/[^\/]*/[^\/]*/[^\/]*/[^\/]*(pcgr|cpsr).html$')
        coverage = Q(path__iregex='umccrise') & Q(path__iregex='(normal|tumor).cacao.html$')
        circos = (Q(path__iregex='umccrise/[^\/]*/[^\/]*/[^(work)*]') & Q(path__iregex='purple/')
                  & Q(path__iregex='circos') & Q(path__iregex='baf') & Q(path__iregex='.png$'))

        wts_bam = Q(path__iregex='wts') & Q(path__iregex='tumor') & Q(path__iregex='.bam$')
        wts_qc = Q(path__iregex='wts') & Q(path__iregex='tumor') & Q(path__iregex='multiqc') & Q(path__iregex='.html$')
        rnasum = Q(path__iregex='rnasum') & Q(path__iregex='RNAseq_report.html$')

        gpl = Q(path__iregex='gridss_purple_linx') & Q(path__iregex='linx.html$')

        tso_ctdna_bam = Q(path__iregex='tso') & Q(path__iregex='ctdna') & Q(path__iregex='.bam$')
        tso_ctdna_vcf = Q(path__iregex='tso') & Q(path__iregex='ctdna') & Q(path__iregex='.vcf.gz$')

        q_results: Q = (bam | vcf | cancer | qc | pcgr | coverage | circos | wts_bam | wts_qc | rnasum | gpl
                        | tso_ctdna_bam | tso_ctdna_vcf)

        qs = qs.filter(q_results)

        volume_name = kwargs.get('volume_name', None)
        if volume_name:
            qs = qs.filter(volume_name__iexact=volume_name)

        return qs


class GDSFile(models.Model):
    """
    Model wrap around IAP GDS File - GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    NOTE:
    For composite (unique) key, it follows S3 style bucket + key pattern, see unique_hash.
    i.e. gds://volume_name/path ~ s3://bucket/key and, this full path must be unique globally.
    """
    id = models.BigAutoField(primary_key=True)
    file_id = models.CharField(max_length=255)
    name = models.TextField()
    volume_id = models.CharField(max_length=255)
    volume_name = models.TextField()
    type = models.CharField(max_length=255, null=True, blank=True)
    tenant_id = models.CharField(max_length=255)
    sub_tenant_id = models.CharField(max_length=255)
    path = models.TextField()
    time_created = models.DateTimeField()
    created_by = models.CharField(max_length=255)
    time_modified = models.DateTimeField()
    modified_by = models.CharField(max_length=255)
    inherited_acl = models.TextField(null=True, blank=True)
    urn = models.TextField()
    size_in_bytes = models.BigIntegerField()
    is_uploaded = models.BooleanField(null=True)
    archive_status = models.CharField(max_length=255)
    time_archived = models.DateTimeField(null=True, blank=True)
    storage_tier = models.CharField(max_length=255)
    presigned_url = models.TextField(null=True, blank=True)
    unique_hash = HashField(unique=True, base_fields=['volume_name', 'path'], default=None)

    objects = GDSFileManager()

    def __str__(self):
        return f"File '{self.name}' in volume '{self.volume_name}' with path '{self.path}'"
