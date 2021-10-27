import logging

from django.db import models, connection
from django.db.models import QuerySet, Value
from django.db.models.functions import Concat

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


class LabMetadataPhenotype(models.TextChoices):
    N_CONTROL = "negative-control"
    NORMAL = "normal"
    TUMOR = "tumor"


class LabMetadataAssay(models.TextChoices):
    AG_SS_CRE = "AgSsCRE"
    CT_TSO = "ctTSO"
    NEB_DNA = "NebDNA"
    NEB_DNA_U = "NebDNAu"
    NEB_RNA = "NebRNA"
    PCR_FREE = "PCR-Free-Tagmentation"
    TEN_X_3PRIME = "10X-3prime-expression"
    TEN_X_5PRIME = "10X-5prime-expression"
    TEN_X_ATAC = "10X-ATAC"
    TEN_X_CITE_FEAT = "10X-CITE-feature"
    TEN_X_CITE_HASH = "10X-CITE-hashing"
    TEN_X_CNV = "10X-CNV"
    TEN_X_VDJ = "10X-VDJ"
    TEN_X_VDJ_TCR = "10X-VDJ-TCR"
    TSO_DNA = "TSODNA"
    TSO_RNA = "TSORNA"
    TSQ_NANO = "TsqNano"
    TSQ_STR = "TsqSTR"


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


class LabMetadataWorkflow(models.TextChoices):
    BCL = "bcl"
    CLINICAL = "clinical"
    CONTROL = "control"
    MANUAL = "manual"
    QC = "qc"
    RESEARCH = "research"


class LabMetadataManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        subject_id = kwargs.get('subject_id', None)
        if subject_id:
            qs = qs.filter(subject_id__iexact=subject_id)

        sample_id = kwargs.get('sample_id', None)
        if sample_id:
            qs = qs.filter(sample_id__iexact=sample_id)

        library_id = kwargs.get('library_id', None)
        if library_id:
            qs = qs.filter(library_id__iexact=library_id)

        phenotype = kwargs.get('phenotype', None)
        if phenotype:
            qs = qs.filter(phenotype__iexact=phenotype)

        type_ = kwargs.get('type', None)
        if type_:
            qs = qs.filter(type__iexact=type_)

        workflow = kwargs.get('workflow', None)
        if workflow:
            qs = qs.filter(workflow__iexact=workflow)

        project_name = kwargs.get('project_name', None)
        if project_name:
            qs = qs.filter(project_name__iexact=project_name)

        project_owner = kwargs.get('project_owner', None)
        if project_owner:
            qs = qs.filter(project_owner__iexact=project_owner)

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

        return qs

    def get_by_sample_library_name(self, sample_library_name) -> QuerySet:
        """
        Here we project (or annotate) virtual attribute called "sample_library_name" which is using database built-in
        concat function of two existing columns sample_id and library_id.

        :param sample_library_name:
        :return: QuerySet
        """
        qs: QuerySet = self.annotate(sample_library_name=Concat('sample_id', Value('_'), 'library_id'))
        qs = qs.filter(sample_library_name__iexact=sample_library_name)
        return qs


class LabMetadata(models.Model):
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
    sample_name = models.CharField(max_length=255)
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
        return 'id=%s, library_id=%s, sample_id=%s, sample_name=%s, subject_id=%s' \
               % (self.id, self.library_id, self.sample_id, self.sample_name, self.subject_id)

    @classmethod
    def get_table_name(cls):
        return cls._meta.db_table

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {cls.get_table_name()};")
