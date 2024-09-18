import logging
import random
from typing import List

from django.db import models
from django.db.models import Max, QuerySet, Q
from libumccr import libregex

from data_portal.exceptions import RandSamplesTooLarge
from data_portal.fields import HashField
from data_portal.models import LabMetadata
from data_portal.models.labmetadata import LabMetadataAssay

logger = logging.getLogger(__name__)


def _strip_topup_rerun_from_library_id_list(library_id_list: List[str]) -> List[str]:
    """
    TODO copy from liborca, perhaps refactor to libumccr
    """
    rglb_id_set = set()
    for library_id in library_id_list:
        # Strip _topup
        rglb = libregex.SAMPLE_REGEX_OBJS['topup'].split(library_id, 1)[0]

        # Strip _rerun
        rglb = libregex.SAMPLE_REGEX_OBJS['rerun'].split(rglb, 1)[0]

        rglb_id_set.add(rglb)

    return list(rglb_id_set)


class S3ObjectManager(models.Manager):
    """
    Manager class for S3 objects, providing additional helper methods.
    """
    MAX_RAND_SAMPLES_LIMIT = 500

    def random_samples(self, required: int) -> QuerySet:
        """
        Obtain random samples as a lazy-loading query set.
        Follows the suggested practice from https://books.agiliq.com/projects/django-orm-cookbook/en/latest/random.html.
        :param required: the required number of samples
        :return: list of random S3OBject samples
        """
        total = self.count()

        if required > self.MAX_RAND_SAMPLES_LIMIT:
            raise RandSamplesTooLarge(self.MAX_RAND_SAMPLES_LIMIT)

        # We can't give more samples than the maximum of what we have
        actual = min(total, required)
        count = 1
        max_id = self.aggregate(max_id=Max("id"))['max_id']
        sample_ids = []

        while count <= actual:
            pk = random.randint(1, max_id)
            query = self.filter(pk=pk)

            if query.exists():
                sample_ids.append(pk)
                count += 1

        return self.filter(id__in=sample_ids)

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.exclude(key__contains='.snakemake')

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket__exact=bucket)

        subject = kwargs.get('subject', None)
        if subject:
            qs = qs.filter(key__icontains=subject)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(key__icontains=run)

        key = kwargs.get('key', None)
        if key:
            qs = qs.filter(key__exact=key)

        return qs

    def get_subject_results(self, subject_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(key__icontains=subject_id).exclude(key__contains='.snakemake')
        bam = Q(key__iregex='wgs') & Q(key__iregex='ready') & Q(key__iregex='.bam$')
        vcf = (Q(key__iregex='umccrised/[^(work)*]') & Q(key__iregex='small_variants/[^\/]*(.vcf.gz$|.maf$)'))
        cancer = Q(key__iregex='umccrised') & Q(key__iregex='cancer_report.html$')
        qc = Q(key__iregex='umccrised') & Q(key__iregex='multiqc_report.html$')
        pcgr = Q(key__iregex='umccrised/[^\/]*/[^\/]*(pcgr|cpsr).html$')
        coverage = Q(key__iregex='umccrised/[^\/]*/[^\/]*(normal|tumor).cacao.html$')
        circos = (Q(key__iregex='umccrised/[^(work)*]') & Q(key__iregex='purple/') & Q(key__iregex='circos')
                  & Q(key__iregex='baf') & Q(key__iregex='.png$'))
        wts_bam = Q(key__iregex='wts') & Q(key__iregex='ready') & Q(key__iregex='.bam$')
        wts_qc = Q(key__iregex='wts') & Q(key__iregex='multiqc/') & Q(key__iregex='multiqc_report.html$')
        wts_fusions = Q(key__iregex='wts') & Q(key__iregex='fusions') & Q(key__iregex='.pdf$')
        rnasum = Q(key__iregex='RNAseq_report.html$')
        gpl = Q(key__iregex='wgs') & Q(key__iregex='gridss_purple_linx') & Q(key__iregex='linx.html$')
        q_results: Q = (bam | vcf | cancer | qc | pcgr | coverage | circos | wts_bam | wts_qc | wts_fusions
                        | rnasum | gpl)
        qs = qs.filter(q_results)

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
        return qs

    def get_subject_sash_results(self, subject_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(key__icontains=subject_id).filter(key__icontains="/sash/")

        cancer = Q(key__iregex='cancer_report.html$')
        pcgr = Q(key__iregex='pcgr.html$')
        cpsr = Q(key__iregex='cpsr.html$')
        linx = Q(key__iregex='linx.html$')
        circos = Q(key__iregex='circos_baf.png$')
        multiqc = Q(key__iregex='multiqc') & Q(key__iregex='.html$')

        vcf_germline_snv = Q(key__iregex='smlv_germline') & Q(key__iregex='.annotations.vcf.gz$')
        vcf_somatic_snv_filter_set = Q(key__iregex='smlv_somatic') & Q(key__iregex='.filters_set.vcf.gz$')
        vcf_somatic_snv_filter_applied = Q(key__iregex='smlv_somatic') & Q(key__iregex='^((?!pcgr).)+$') & \
                                         Q(key__iregex='.pass.vcf.gz$')
        vcf_somatic_sv = Q(key__iregex='sv_somatic') & Q(key__iregex='sv.prioritised.vcf.gz$')

        q_results: Q = (
                cancer | pcgr | cpsr | circos | linx | vcf_germline_snv | multiqc |
                vcf_somatic_snv_filter_set | vcf_somatic_snv_filter_applied | vcf_somatic_sv
        )

        qs = qs.filter(q_results)

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
        return qs

    def get_subject_cttsov2_results(self, subject_id: str, **kwargs) -> QuerySet:
        # We are to show analysis done by cttsov2 pipeline from ICA v2 BYOB bucket
        # Get both ctTSO and ctTSOv2 libraries of the Subject
        subject_meta_list: List[LabMetadata] = LabMetadata.objects.filter(
            subject_id=subject_id,
            assay__in=[str(LabMetadataAssay.CT_TSO_V2.value).lower(), str(LabMetadataAssay.CT_TSO.value).upper()],
        ).all()

        cttsov2_libraries: List[str] = list()
        for meta in subject_meta_list:
            cttsov2_libraries.append(meta.library_id)

        # strip library suffixes
        minted_cttsov2_libraries = _strip_topup_rerun_from_library_id_list(cttsov2_libraries)

        # if the subject_id has no cttsov2 assay library then skip all together
        if not minted_cttsov2_libraries:
            return self.none()

        # baseline queryset
        qs: QuerySet = self.filter(key__icontains="/cttsov2/")

        # TODO
        #  for baseline queryset, we can also consider bucket filter for a tad more performance boost
        #  but this also makes dependency upon bucket name look up
        #  anyway, unlike Athena; Django to vanilla SQL query on a RDBMS table is already fast enough with index lookup
        #  we can observe current approach and explore this down the track

        # create library filter Q
        lib_q = Q()
        for lib in minted_cttsov2_libraries:
            lib_q.add(data=Q(key__icontains=lib), conn_type=Q.OR)

        # create file of interest Q
        tmb_metrics_csv_q = Q(key__iregex='tmb.metrics.csv$')
        all_bam_q = Q(key__iregex='.bam$')
        all_results_q = Q(key__icontains='/Results/')

        q_results: Q = (
            tmb_metrics_csv_q | all_results_q | all_bam_q
        ) & lib_q

        qs = qs.filter(q_results)

        return qs


class S3Object(models.Model):
    """
    MySQL has character length limitation on unique indexes and since key column require to store
    lengthy path, unique_hash is sha256sum of bucket and key for unique indexes purpose.
    """
    id = models.BigAutoField(primary_key=True)
    bucket = models.CharField(max_length=255)
    key = models.TextField()
    size = models.BigIntegerField()
    last_modified_date = models.DateTimeField()
    e_tag = models.CharField(max_length=255)
    unique_hash = HashField(unique=True, base_fields=['bucket', 'key'], default=None)

    SORTABLE_COLUMNS = ['size', 'last_modified_date']
    DEFAULT_SORT_COL = 'last_modified_date'

    objects = S3ObjectManager()
