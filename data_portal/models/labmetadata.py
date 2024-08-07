import logging

from django.db import models, connection
from django.db.models import QuerySet, Value
from django.db.models.aggregates import Count
from django.db.models.functions import Concat

from data_portal.models.base import PortalBaseModel, PortalBaseManager
from data_portal.models.libraryrun import LibraryRun

logger = logging.getLogger(__name__)


class LabMetadataType(models.TextChoices):
    CT_DNA = "ctDNA"
    CT_TSO = "ctTSO"
    EXOME = "exome"
    OTHER = "other"
    TEN_X = "10X"
    TSO_DNA = "TSO-DNA"
    TSO_RNA = "TSO-RNA"
    WGS = "WGS"
    WTS = "WTS"
    BI_MODAL = "BiModal"
    ME_DIP = "MeDIP"
    METAGENM = "Metagenm"
    METHYL_SEQ = "MethylSeq"


class LabMetadataPhenotype(models.TextChoices):
    N_CONTROL = "negative-control"
    NORMAL = "normal"
    TUMOR = "tumor"


class LabMetadataAssay(models.TextChoices):
    TSQ_NANO = "TsqNano"
    TSQ_STR = "TsqSTR"
    NEB_DNA = "NebDNA"
    NEB_RNA = "NebRNA"
    TEN_X_3PRIME = "10X-3prime-expression"
    TEN_X_5PRIME = "10X-5prime-expression"
    TEN_X_ADT = "10X-ADT"
    TEN_X_ATAC = "10X-ATAC"
    TEN_X_CITE_FEAT = "10X-CITE-feature"
    TEN_X_CITE_HASH = "10X-CITE-hashing"
    TEN_X_CNV = "10X-CNV"
    TEN_X_CSP = "10X-CSP"
    TEN_X_GEX = "10X-GEX"
    TEN_X_VDJ = "10X-VDJ"
    TEN_X_VDJ_BCR = "10X-VDJ-BCR"
    TEN_X_VDJ_TCR = "10X-VDJ-TCR"
    AG_SS_CRE = "AgSsCRE"
    B_ATAC = "bATAC"
    CRISPR = "CRISPR"
    CT_TSO = "ctTSO"
    IDT_X_GEN = "IDTxGen"
    ILMN_DNA_PREP = "IlmnDNAprep"
    NEB_DNA_U = "NebDNAu"
    NEB_MS = "NebMS"
    PCR_FREE = "PCR-Free-Tagmentation"
    TAKARA = "Takara"
    TPL_X_HV = "TPlxHV"
    TSO_DNA = "TSODNA"
    BM_5L = "BM-5L"
    BM_6L = "BM-6L"
    ME_DIP = "MeDIP"
    CT_TSO_V2 = "ctTSOv2"


class LabMetadataQuality(models.TextChoices):
    BORDERLINE = "borderline"
    GOOD = "good"
    POOR = "poor"
    VERY_POOR = "VeryPoor"


class LabMetadataSource(models.TextChoices):
    ACITES = "ascites"
    BLOOD = "blood"
    BONE = "bone-marrow"
    BUCCAL = "buccal"
    CELL_LINE = "cell-line"
    CF_DNA = "cfDNA"
    CYST = "cyst-fluid"
    DNA = "DNA"
    EYEBROW = "eyebrow-hair"
    FFPE = "FFPE"
    FNA = "FNA"
    OCT = "OCT"
    ORGANOID = "organoid"
    PDX = "PDX-tissue"
    PLASMA = "plasma-serum"
    RNA = "RNA"
    TISSUE = "tissue"
    WATER = "water"
    SKIN = "skin"


class LabMetadataWorkflow(models.TextChoices):
    BCL = "bcl"
    CLINICAL = "clinical"
    CONTROL = "control"
    MANUAL = "manual"
    QC = "qc"
    RESEARCH = "research"


def remove_not_sequenced(qs: QuerySet) -> QuerySet:
    # filter metadata to those entries that were sequenced, i.e. have a LibraryRun entry
    inner_qs = LibraryRun.objects.values_list('library_id', flat=True)
    qs = qs.filter(library_id__in=inner_qs)
    return qs


class LabMetadataManager(PortalBaseManager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()

        sequenced = kwargs.pop('sequenced', False)  # take the special sequenced parameter out of the kwargs
        qs = self.get_model_fields_query(qs, **kwargs)

        # if only records for sequenced libs are requested, remove the ones that are not
        if sequenced:
            qs = remove_not_sequenced(qs)

        return qs

    def get_by_keyword_in(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        subjects = kwargs.get('subjects', None)
        if subjects:
            qs = qs.filter(subject_id__in=subjects)

        samples = kwargs.get('samples', None)
        if samples:
            qs = qs.filter(sample_id__in=samples)

        libraries = kwargs.get('libraries', None)
        if libraries:
            qs = qs.filter(library_id__in=libraries)

        phenotypes = kwargs.get('phenotypes', None)
        if phenotypes:
            qs = qs.filter(phenotype__in=phenotypes)

        types = kwargs.get('types', None)
        if types:
            qs = qs.filter(type__in=types)

        workflows = kwargs.get('workflows', None)
        if workflows:
            qs = qs.filter(workflow__in=workflows)

        project_names = kwargs.get('project_names', None)
        if project_names:
            qs = qs.filter(project_name__in=project_names)

        project_owners = kwargs.get('project_owners', None)
        if project_owners:
            qs = qs.filter(project_owner__in=project_owners)

        sequenced = kwargs.get('sequenced', False)
        if sequenced:
            qs = remove_not_sequenced(qs)

        return qs

    def get_by_sample_library_name(self, sample_library_name, sequenced: bool = False) -> QuerySet:
        """
        Here we project (or annotate) virtual attribute called "sample_library_name" which is using database built-in
        concat function of two existing columns sample_id and library_id.

        :param sample_library_name:
        :param sequenced: Boolean to indicate whether to only return metadata for sequenced libraries
        :return: QuerySet
        """
        qs: QuerySet = self.annotate(sample_library_name=Concat('sample_id', Value('_'), 'library_id'))
        qs = qs.filter(sample_library_name__iexact=sample_library_name)

        if sequenced:
            qs = remove_not_sequenced(qs)

        return qs

    def get_by_aggregate_count(self, field):
        return self.values(field).annotate(count=Count(field)).order_by(field)

    def get_by_cube(self, field_left, field_right, field_sort):
        return self.values(field_left, field_right).annotate(count=Count(1)).order_by(field_sort)


class LabMetadata(PortalBaseModel):
    """
    Models a row in the lab tracking sheet data. Fields are the columns.
    """

    # Portal internal auto incremental PK ID. Scheme may change as need be and may rebuild thereof.
    # External system or business logic should not rely upon this ID field.
    # Use any of unique fields or <>_id fields below.
    id = models.BigAutoField(primary_key=True)

    # TODO: as far as Clarity is concerned, "external" lib id = tracking sheet.
    #  do we want to store clarity-generated lib id, and what do we want to call it?
    # external_library_id = models.CharField(max_length=255)

    library_id = models.CharField(max_length=255, unique=True)
    sample_name = models.CharField(max_length=255, null=True, blank=True)
    sample_id = models.CharField(max_length=255)
    external_sample_id = models.CharField(max_length=255, null=True, blank=True)
    subject_id = models.CharField(max_length=255, null=True, blank=True)
    external_subject_id = models.CharField(max_length=255, null=True, blank=True)
    phenotype = models.CharField(choices=LabMetadataPhenotype.choices, max_length=255)
    quality = models.CharField(choices=LabMetadataSource.choices, max_length=255)
    source = models.CharField(choices=LabMetadataSource.choices, max_length=255)
    project_name = models.CharField(max_length=255, null=True, blank=True)
    project_owner = models.CharField(max_length=255, null=True, blank=True)
    experiment_id = models.CharField(max_length=255, null=True, blank=True)
    type = models.CharField(choices=LabMetadataType.choices, max_length=255)
    assay = models.CharField(choices=LabMetadataAssay.choices, max_length=255)
    override_cycles = models.CharField(max_length=255, null=True, blank=True)
    workflow = models.CharField(choices=LabMetadataWorkflow.choices, max_length=255)
    coverage = models.CharField(max_length=255, null=True, blank=True)
    truseqindex = models.CharField(max_length=255, null=True, blank=True)

    objects = LabMetadataManager()

    def __str__(self):
        return 'id=%s, library_id=%s, sample_id=%s, subject_id=%s' \
               % (self.id, self.library_id, self.sample_id, self.subject_id)

    @classmethod
    def get_table_name(cls):
        return cls._meta.db_table

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {cls.get_table_name()};")
