import logging
import random
import re
from typing import List

from django.db import models
from django.db.models import Max, QuerySet, Q
from libumccr import libregex

from data_portal.exceptions import RandSamplesTooLarge
from data_portal.fields import HashField, HashFieldHelper
from data_portal.models import LabMetadata
from data_portal.models.labmetadata import LabMetadataAssay, LabMetadataType

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
        vcf = (Q(key__iregex='umccrised/[^(work)*]') & Q(key__iregex=r'small_variants/[^\/]*(.vcf.gz$|.maf$)'))
        cancer = Q(key__iregex='umccrised') & Q(key__iregex='cancer_report.html$')
        qc = Q(key__iregex='umccrised') & Q(key__iregex='multiqc_report.html$')
        pcgr = Q(key__iregex=r'umccrised/[^\/]*/[^\/]*(pcgr|cpsr).html$')
        coverage = Q(key__iregex=r'umccrised/[^\/]*/[^\/]*(normal|tumor).cacao.html$')
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

    def get_byob(self):
        """
        new era analysis result output buckets (orchestrated by orcabus)
        we harmonise all outputs into single bucket in each environment
        buckets are name by following a convention, hence, we can statically look up

        implNote:
            bucket name column is indexed and, act like a `SortKey` to reduce the amount of data rows being scanned
        """
        bucket_dict = self.filter(bucket__in=[
            'pipeline-dev-cache-503977275616-ap-southeast-2',
            'pipeline-prod-cache-503977275616-ap-southeast-2',
            'pipeline-stg-cache-503977275616-ap-southeast-2',
        ]).values('bucket').distinct().first()
        if bucket_dict:
            return bucket_dict['bucket']
        return None

    def get_subject_cttsov2_results_from_icav2(self, subject_id: str, **kwargs) -> QuerySet:
        # We are to show analysis done by cttsov2 pipeline from ICA v2 BYOB bucket
        # Get both ctTSO and ctTSOv2 libraries of the Subject
        subject_meta_list: List[LabMetadata] = LabMetadata.objects.filter(
            subject_id=subject_id,
            assay__in=[LabMetadataAssay.CT_TSO_V2.value, LabMetadataAssay.CT_TSO.value],
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

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)

        exclude_uq_hashes = kwargs.get('exclude_uq_hashes', [])  # exclude migrated (relocated) objects

        qs = qs.filter(q_results).exclude(unique_hash__in=exclude_uq_hashes)

        return qs

    def get_subject_wgts_results_from_icav2(self, subject_id: str, **kwargs) -> QuerySet:
        # We are to show analysis done by WGTS pipelines from ICA v2 BYOB bucket
        # Get both WGS and WTS libraries of the Subject
        subject_meta_list: List[LabMetadata] = LabMetadata.objects.filter(
            subject_id=subject_id,
            type__in=[LabMetadataType.WGS.value, LabMetadataType.WTS.value],
        ).all()

        wgts_libraries: List[str] = list()
        for meta in subject_meta_list:
            wgts_libraries.append(meta.library_id)

        # strip library suffixes
        minted_wgts_libraries = _strip_topup_rerun_from_library_id_list(wgts_libraries)

        # if the subject_id has no wgts library then skip all together
        if not minted_wgts_libraries:
            return self.none()

        # baseline queryset filter in the key
        baseline = (
                Q(key__icontains='/analysis/umccrise/') |
                Q(key__icontains='/analysis/tumor_normal/') |
                Q(key__icontains='/analysis/tumor-normal/') |
                Q(key__icontains='/analysis/wts/') |
                Q(key__icontains='/analysis/rnasum/')
        )

        # create library filter Q
        lib_q = Q()
        for lib in minted_wgts_libraries:
            lib_q.add(data=Q(key__icontains=lib), conn_type=Q.OR)

        bam = Q(key__iregex='tumor') & Q(key__iregex='normal') & Q(key__iregex='.bam$')
        vcf = (Q(key__iregex=r'umccrise/[^\/]*/[^\/]*/[^(work)*]')
               & Q(key__iregex=r'small_variants/[^\/]*(.vcf.gz$|.maf$)'))

        vcf_germline = (
                Q(key__iregex='dragen_germline')
                & Q(key__iregex='.vcf.gz$')
        )

        cancer = Q(key__iregex='umccrise') & Q(key__iregex='cancer_report.html$')
        qc = Q(key__iregex='umccrise') & Q(key__iregex='multiqc_report.html$')
        pcgr = Q(key__iregex=r'umccrise/[^\/]*/[^\/]*/[^\/]*/[^\/]*(pcgr|cpsr).html$')
        coverage = Q(key__iregex='umccrise') & Q(key__iregex='(normal|tumor).cacao.html$')
        circos = (Q(key__iregex=r'umccrise/[^\/]*/[^\/]*/[^(work)*]') & Q(key__iregex='purple/')
                  & Q(key__iregex='circos') & Q(key__iregex='baf') & Q(key__iregex='.png$'))

        wts_bam = Q(key__iregex='wts') & Q(key__iregex='.bam$')
        wts_qc = Q(key__iregex='wts') & Q(key__iregex='multiqc') & Q(key__iregex='.html$')
        wts_fusions = Q(key__iregex='wts') & Q(key__iregex='fusions') & Q(key__iregex='.pdf$')
        rnasum = Q(key__iregex='rnasum') & Q(key__iregex='RNAseq_report.html$')

        q_results: Q = (bam | vcf | vcf_germline | cancer | qc | pcgr | coverage | circos | wts_bam | wts_qc
                        | wts_fusions | rnasum) & lib_q & baseline

        exclude_uq_hashes = kwargs.get('exclude_uq_hashes', [])  # exclude migrated (relocated) objects

        qs = self.filter(q_results).exclude(unique_hash__in=exclude_uq_hashes)

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
        return qs

    def get_subject_sash_results_from_icav2(self, subject_id: str, **kwargs) -> QuerySet:
        # We are to show analysis done by orcabus oncoanalyser pipelines from BYOB bucket
        # Get both WGS and WTS libraries of the Subject
        subject_meta_list: List[LabMetadata] = LabMetadata.objects.filter(
            subject_id=subject_id,
            type__in=[LabMetadataType.WGS.value, LabMetadataType.WTS.value],
        ).all()

        wgts_libraries: List[str] = list()
        for meta in subject_meta_list:
            wgts_libraries.append(meta.library_id)

        # strip library suffixes
        minted_wgts_libraries = _strip_topup_rerun_from_library_id_list(wgts_libraries)

        # if the subject_id has no wgts library then skip all together
        if not minted_wgts_libraries:
            return self.none()

        # baseline queryset filter in the key
        baseline = Q(key__icontains='/analysis/sash/')

        # create library filter Q
        lib_q = Q()
        for lib in minted_wgts_libraries:
            lib_q.add(data=Q(key__icontains=lib), conn_type=Q.OR)

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
        ) & lib_q & baseline

        exclude_uq_hashes = kwargs.get('exclude_uq_hashes', [])  # exclude migrated (relocated) objects

        qs = self.filter(q_results).exclude(unique_hash__in=exclude_uq_hashes)

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
        return qs

    def get_migrated_subject_results_from_icav1(self, subject_id: str, byob_name: str, gds_results_by_subject: QuerySet):
        # workflow type src to dst mapping lookup table
        # see https://github.com/umccr/biodaily/blob/ee9c4f6/data-migration/icav1-to-icav2/clinical-data/Move%20Clinical%20Data.py
        ICAV2_ANALYSIS_NAME_BY_WFR_TYPE = {
            # WGS SOMATIC
            "wgs_tumor_normal": "tumor-normal",
            # WTS
            "wts_tumor_only": "wts",
            # QC
            "wts_alignment_qc": "wgts-qc",
            "wgs_alignment_qc": "wgts-qc",
            # TSO
            "tso_ctdna_tumor_only": "cttsov1",
            # GERMLINE
            "GERMLINE": "germline",
            # UMCCRise
            "umccrise": "umccrise",
            # RNASum
            "rnasum": "rnasum"
        }

        def calc_byob_uq_hash(_bucket_name, _project_name, _dst_type, _rpath):
            byob_key = f"byob-icav2/{_project_name}/analysis/{_dst_type}/{_rpath}"
            h = HashFieldHelper()
            h.add(_bucket_name).add(byob_key)
            byob_uq_hash = h.calculate_hash()
            # print(byob_key, byob_uq_hash)
            return byob_uq_hash

        def calc_archive_uq_hash(_rpath):
            archive_bucket_name = "archive-prod-analysis-503977275616-ap-southeast-2"  # we only have 1 archive as destination
            _pid = _rpath.split("/")[0]
            assert re.match(r"\d{8}[A-Za-z0-9]{8}", _pid) is not None, f"{_pid} is not portal_run_id for {subject_id} - {_rpath}"
            _yyyy = _pid[:4]
            _mm = _pid[4:6]
            archive_bucket_key = f"v1/year={_yyyy}/month={_mm}/{_rpath}"
            h = HashFieldHelper()
            h.add(archive_bucket_name).add(archive_bucket_key)
            archive_uq_hash = h.calculate_hash()
            # print(archive_bucket_key, archive_uq_hash)
            return archive_uq_hash

        cttsov1_uq_hashes = []
        wgts_uq_hashes = []
        src_cnt = 0
        for gdsfile in gds_results_by_subject.all():
            # get gds path as vectorised elements
            gdsfile_path_vec = gdsfile.path.split("/")

            # ImplNote:
            #  The input dataset gdsfile is frozen set. There are no more ongoing update activity nor; its structure has
            #  now considered "fix" constant. No worry with slice operation `gdsfile_path_vec` down under. These were
            #  well-known file path convention from ICAv1. ditto "convention over configuration..." stuff!

            # resolve type
            old_type = gdsfile_path_vec[3]
            new_type = ICAV2_ANALYSIS_NAME_BY_WFR_TYPE[old_type]

            if old_type == "tso_ctdna_tumor_only":
                # need to pop LibraryID out from middle path
                # see https://github.com/umccr/biodaily/blob/ee9c4f6/data-migration/icav1-to-icav2/clinical-data/clinical_data_move_scripts/tso_ctdna_tumor_only/batch_000.sh
                remainder_path = "/".join([gdsfile_path_vec[4]] + gdsfile_path_vec[6:])
                cttsov1_uq_hashes.append(calc_byob_uq_hash(byob_name, gdsfile.volume_name, new_type, remainder_path))
                cttsov1_uq_hashes.append(calc_archive_uq_hash(remainder_path))
            else:
                remainder_path = "/".join(gdsfile_path_vec[4:])
                wgts_uq_hashes.append(calc_byob_uq_hash(byob_name, gdsfile.volume_name, new_type, remainder_path))
                wgts_uq_hashes.append(calc_archive_uq_hash(remainder_path))

            src_cnt += 1

        icav1_cttsov1_results_by_subject_qs = self.filter(unique_hash__in=cttsov1_uq_hashes)
        icav1_wgts_results_by_subject_qs = self.filter(unique_hash__in=wgts_uq_hashes)

        # --- QC step
        # combine uq_hashes and make count check
        # most of the time, this should match up. if not, log warning and follow up manually what have been missed
        uq_hashes = cttsov1_uq_hashes + wgts_uq_hashes
        dst_cnt = self.filter(unique_hash__in=uq_hashes).count()
        if src_cnt != dst_cnt:
            logger.warning(f"MISMATCH {subject_id} -- gds:{src_cnt}, s3:{dst_cnt}")

        return icav1_cttsov1_results_by_subject_qs, icav1_wgts_results_by_subject_qs, uq_hashes


class S3Object(models.Model):
    """
    MySQL has character length limitation on unique indexes and since key column require to store
    lengthy path, unique_hash is sha256sum of bucket and key for unique indexes purpose.
    """
    class Meta:
        indexes = [
            models.Index(fields=['bucket']),
        ]

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
