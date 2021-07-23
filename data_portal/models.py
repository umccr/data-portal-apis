import json
import random
import uuid
from typing import Union

from django.db import models, connection
from django.db.models import Max, QuerySet, Q, Value
from django.db.models.functions import Concat

from data_portal.exceptions import RandSamplesTooLarge
from data_portal.fields import HashField, HashFieldHelper
from data_processors.pipeline.domain.workflow import WorkflowStatus


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
            qs = qs.filter(bucket=bucket)

        subject = kwargs.get('subject', None)
        if subject:
            qs = qs.filter(key__icontains=subject)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(key__icontains=run)

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
        rnasum = Q(key__iregex='RNAseq_report.html$')
        q_results: Q = bam | vcf | cancer | qc | pcgr | coverage | circos | wts_bam | wts_qc | rnasum
        qs = qs.filter(q_results)

        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
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


class LIMSRowManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        subject = kwargs.get('subject', None)
        if subject:
            qs = qs.filter(subject_id__iexact=subject)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(illumina_id__iexact=run)

        return qs


class LIMSRow(models.Model):
    """
    Models a row in the LIMS data. Fields are the columns.
    """

    class Meta:
        unique_together = ['illumina_id', 'library_id']

    id = models.BigAutoField(primary_key=True)
    illumina_id = models.CharField(max_length=255)
    run = models.IntegerField()
    timestamp = models.DateField()
    subject_id = models.CharField(max_length=255, null=True, blank=True)
    sample_id = models.CharField(max_length=255)
    library_id = models.CharField(max_length=255)
    external_subject_id = models.CharField(max_length=255, null=True, blank=True)
    external_sample_id = models.CharField(max_length=255, null=True, blank=True)
    external_library_id = models.CharField(max_length=255, null=True, blank=True)
    sample_name = models.CharField(max_length=255, null=True, blank=True)
    project_owner = models.CharField(max_length=255, null=True, blank=True)
    project_name = models.CharField(max_length=255, null=True, blank=True)
    type = models.CharField(max_length=255, null=True, blank=True)
    assay = models.CharField(max_length=255, null=True, blank=True)
    override_cycles = models.CharField(max_length=255, null=True, blank=True)
    phenotype = models.CharField(max_length=255, null=True, blank=True)
    source = models.CharField(max_length=255, null=True, blank=True)
    quality = models.CharField(max_length=255, null=True, blank=True)
    topup = models.CharField(max_length=255, null=True, blank=True)
    secondary_analysis = models.CharField(max_length=255, null=True, blank=True)
    workflow = models.CharField(max_length=255, null=True, blank=True)
    fastq = models.TextField(null=True, blank=True)
    number_fastqs = models.CharField(max_length=255, null=True, blank=True)
    results = models.TextField(null=True, blank=True)
    trello = models.TextField(null=True, blank=True)
    notes = models.TextField(null=True, blank=True)
    todo = models.CharField(max_length=255, null=True, blank=True)

    objects = LIMSRowManager()

    def __str__(self):
        return 'id=%s, illumina_id=%s, sample_id=%s, sample_name=%s, subject_id=%s' \
               % (self.id, self.illumina_id, self.sample_id, self.sample_name, self.subject_id)

    # The attributes used to find linking between LIMSRow and S3Object(Key)
    # In short, these attr values should be part of the S3 Object key.
    # This is the common logic used for both persisting s3 and lims.
    S3_LINK_ATTRS = ('subject_id', 'sample_id')


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


class S3LIMS(models.Model):
    """
    Models the association between a S3 object and a LIMS row
    """

    class Meta:
        unique_together = ['s3_object', 'lims_row']

    id = models.BigAutoField(primary_key=True)
    s3_object = models.ForeignKey(S3Object, on_delete=models.CASCADE)
    lims_row = models.ForeignKey(LIMSRow, on_delete=models.CASCADE)


class Configuration(models.Model):
    """
    Model that stores a configuration value
    Currently not used; but could be used for cases when we want to record the state of some data - e.g. LIMS file.
    """
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255, unique=True)
    value = models.TextField()

    @staticmethod
    def get(name: str) -> Union['Configuration', None]:
        """
        Get configuration object by name. None if not exists
        """
        query_set = Configuration.objects.filter(name=name)
        if query_set.exists():
            return query_set.get()
        return None

    @staticmethod
    def set(name: str, val: str) -> None:
        """
        Set config value by name. Will create one if not exist.
        """
        config = Configuration.get(name)

        if config:
            config.value = val
        else:
            config = Configuration(name=name, value=val)

        config.save()

    @staticmethod
    def same_or_update(name: str, val: str):
        """
        Compares val with current config value if exist.
        Update if config not exist or the two values are not equal.
        :return True if two values are same; False if they are different or current val does not exist.
        """
        config = Configuration.get(name)

        if config is None or config.value != val:
            Configuration.set(name, val)
            return False

        return True


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


class Sequence(models.Model):
    class Meta:
        unique_together = ['instrument_run_id', 'run_id']

    id = models.BigAutoField(primary_key=True)
    instrument_run_id = models.CharField(max_length=255)
    run_id = models.CharField(max_length=255)
    run_config = models.JSONField(null=True, blank=True)  # could be it's own model
    sample_sheet_name = models.CharField(max_length=255)
    sample_sheet_config = models.JSONField(null=True, blank=True)  # could be it's own model
    gds_folder_path = models.CharField(max_length=255)
    gds_volume_name = models.CharField(max_length=255)
    reagent_barcode = models.CharField(max_length=255)
    flowcell_barcode = models.CharField(max_length=255)
    status = models.CharField(max_length=255)


class SequenceRunManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        run_id = kwargs.get('run_id', None)
        if run_id:
            qs = qs.filter(run_id__iexact=run_id)

        name = kwargs.get('name', None)
        if name:
            qs = qs.filter(name__iexact=name)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(name__iexact=run)

        instrument_run_id = kwargs.get('instrument_run_id', None)
        if instrument_run_id:
            qs = qs.filter(instrument_run_id__iexact=instrument_run_id)

        status = kwargs.get('status', None)
        if status:
            qs = qs.filter(status__iexact=status)

        return qs


class SequenceRun(models.Model):
    class Meta:
        unique_together = ['run_id', 'date_modified', 'status']

    id = models.BigAutoField(primary_key=True)
    sequence = models.ForeignKey(Sequence, on_delete=models.SET_NULL, null=True, blank=True)  # could simply be linked by instrument_run_id
    run_id = models.CharField(max_length=255)
    date_modified = models.DateTimeField()
    status = models.CharField(max_length=255)
    gds_folder_path = models.TextField()
    gds_volume_name = models.TextField()
    reagent_barcode = models.CharField(max_length=255)
    v1pre3_id = models.CharField(max_length=255)
    acl = models.TextField()
    flowcell_barcode = models.CharField(max_length=255)
    sample_sheet_name = models.CharField(max_length=255)
    api_url = models.TextField()
    name = models.CharField(max_length=255)
    instrument_run_id = models.CharField(max_length=255)
    msg_attr_action = models.CharField(max_length=255)
    msg_attr_action_type = models.CharField(max_length=255)
    msg_attr_action_date = models.DateTimeField()
    msg_attr_produced_by = models.CharField(max_length=255)

    objects = SequenceRunManager()

    def __str__(self):
        return f"Run ID '{self.run_id}', " \
               f"Name '{self.name}', " \
               f"Instrument ID '{self.instrument_run_id}', " \
               f"Date Modified '{self.date_modified}', " \
               f"Status '{self.status}'"


class FastqListRowManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(sequence_run__instrument_run_id__exact=run)

        rgid = kwargs.get('rgid', None)
        if rgid:
            qs = qs.filter(rgid__iexact=rgid)

        rgsm = kwargs.get('rgsm', None)
        if rgsm:
            qs = qs.filter(rgsm__iexact=rgsm)

        rglb = kwargs.get('rglb', None)
        if rglb:
            qs = qs.filter(rglb__iexact=rglb)

        lane = kwargs.get('lane', None)
        if lane:
            qs = qs.filter(lane__exact=lane)

        project_owner = kwargs.get('project_owner', None)
        if project_owner:
            qs_meta = LabMetadata.objects.filter(project_owner__iexact=project_owner).values("library_id")
            qs = qs.filter(rglb__in=qs_meta)

        return qs


class FastqListRow(models.Model):   
    
    class Meta:
        unique_together = ['rgid']

    id = models.BigAutoField(primary_key=True)
    rgid = models.CharField(max_length=255)
    rgsm = models.CharField(max_length=255)
    rglb = models.CharField(max_length=255)
    lane = models.IntegerField()
    read_1 = models.TextField()
    read_2 = models.TextField(null=True, blank=True)  # This is nullable. Search 'read_2' in fastq_list_row.handler()

    sequence_run = models.ForeignKey(SequenceRun, on_delete=models.SET_NULL, null=True, blank=True)

    objects = FastqListRowManager()

    def __str__(self):
        return f"RGID: {self.rgid}, RGSM: {self.rgsm}, RGLB: {self.rglb}"

    def as_dict(self):
        """as-is in dict i.e., no transformation"""
        d = {
            "rgid": self.rgid,
            "rglb": self.rglb,
            "rgsm": self.rgsm,
            "lane": self.lane,
            "read_1": self.read_1,
            "read_2": self.read_2
        }
        return d

    def as_json(self):
        return json.dumps(self.as_dict())

    def to_dict(self):
        dict_obj = {
            "rgid": self.rgid,
            "rglb": self.rglb,
            "rgsm": self.rgsm,
            "lane": self.lane,
            "read_1": self.read_1_to_dict()
        }
        if self.read_2:
            dict_obj["read_2"] = self.read_2_to_dict()
        return dict_obj

    def __json__(self):
        return json.dumps(self.to_dict())

    def read_1_to_dict(self):
        return {"class": "File", "location": self.read_1}

    def read_2_to_dict(self):
        return {"class": "File", "location": self.read_2}


class Batch(models.Model):
    class Meta:
        unique_together = ['name', 'created_by']

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255)
    created_by = models.CharField(max_length=255)
    context_data = models.TextField(null=True, blank=True)

    def __str__(self):
        return f"ID: {self.id}, NAME: {self.name}, CREATED_BY: {self.created_by}"


class BatchRun(models.Model):
    id = models.BigAutoField(primary_key=True)
    batch = models.ForeignKey(Batch, on_delete=models.SET_NULL, null=True, blank=True)
    step = models.CharField(max_length=255)
    running = models.BooleanField(null=True, blank=True)
    notified = models.BooleanField(null=True, blank=True)

    def __str__(self):
        return f"ID: {self.id}, STEP: {self.step}, RUNNING: {self.running}, " \
               f"NOTIFIED: {self.notified}, BATCH_ID: {self.batch.id}"


class WorkflowManager(models.Manager):

    def get_by_batch_run(self, batch_run: BatchRun) -> QuerySet:
        qs: QuerySet = self.filter(batch_run=batch_run)
        return qs

    def get_running_by_batch_run(self, batch_run: BatchRun) -> QuerySet:
        qs: QuerySet = self.filter(
            batch_run=batch_run,
            start__isnull=False,
            end__isnull=True,
            end_status__icontains=WorkflowStatus.RUNNING.value
        )
        return qs

    def get_completed_by_batch_run(self, batch_run: BatchRun) -> QuerySet:
        qs: QuerySet = self.filter(
            batch_run=batch_run,
            start__isnull=False,
        ).exclude(end_status__icontains=WorkflowStatus.RUNNING.value)
        return qs

    def find_by_idempotent_matrix(self, **kwargs):
        """
        search workflow using: Workflow type_name, wfl_id, version, sample_name, sqr, batch_run_id
        return any workflow matching: end=NULL && end_status=NULL && start=NOT_NULL && input=NOT_NULL
        """
        type_name = kwargs.get('type_name')
        wfl_id = kwargs.get('wfl_id')
        version = kwargs.get('version')
        sample_name = kwargs.get('sample_name')
        sequence_run = kwargs.get('sequence_run')
        batch_run = kwargs.get('batch_run')

        qs: QuerySet = self.filter(
            type_name=type_name,
            wfl_id=wfl_id,
            version=version,
            sample_name=sample_name,
            sequence_run=sequence_run,
            batch_run=batch_run,
            end__isnull=True,
            end_status__isnull=True,  # TODO: Why END_status? Is that ever true? Is a workflow not initially set to "Running"??
            start__isnull=False,
            input__isnull=False,
        )
        return qs

    def get_running_by_sequence_run(self, sequence_run: SequenceRun, type_name: str) -> QuerySet:
        qs: QuerySet = self.filter(
            sequence_run=sequence_run,
            type_name__iexact=type_name.lower(),
            end__isnull=True
        )
        return qs

    def get_succeeded_by_sequence_run(self, sequence_run: SequenceRun, type_name: str) -> QuerySet:
        qs: QuerySet = self.filter(
            sequence_run=sequence_run,
            type_name__iexact=type_name.lower(),
            end__isnull=False,
            end_status__iexact=WorkflowStatus.SUCCEEDED.value
        )
        return qs

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        sequence_run = kwargs.get('sequence_run', None)
        if sequence_run:
            qs = qs.filter(sequence_run_id__exact=sequence_run)

        sequence = kwargs.get('sequence', None)
        if sequence:
            qs = qs.filter(sequence_run__name__iexact=sequence)

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(sequence_run__name__iexact=run)

        sample_name = kwargs.get('sample_name', None)
        if sample_name:
            qs = qs.filter(sample_name__iexact=sample_name)

        type_name = kwargs.get('type_name', None)
        if type_name:
            qs = qs.filter(type_name__iexact=type_name)

        end_status = kwargs.get('end_status', None)
        if end_status:
            qs = qs.filter(end_status__iexact=end_status)

        return qs


class Workflow(models.Model):
    class Meta:
        unique_together = ['wfr_id', 'wfl_id', 'wfv_id']

    id = models.BigAutoField(primary_key=True)
    wfr_name = models.TextField(null=True, blank=True)
    sample_name = models.CharField(max_length=255, null=True, blank=True)
    type_name = models.CharField(max_length=255)
    wfr_id = models.CharField(max_length=255)
    wfl_id = models.CharField(max_length=255)
    wfv_id = models.CharField(max_length=255)
    version = models.CharField(max_length=255)
    input = models.TextField()
    start = models.DateTimeField()
    output = models.TextField(null=True, blank=True)
    end = models.DateTimeField(null=True, blank=True)
    end_status = models.CharField(max_length=255, null=True, blank=True)
    notified = models.BooleanField(null=True, blank=True)
    sequence_run = models.ForeignKey(SequenceRun, on_delete=models.SET_NULL, null=True, blank=True)
    batch_run = models.ForeignKey(BatchRun, on_delete=models.SET_NULL, null=True, blank=True)

    objects = WorkflowManager()

    def __str__(self):
        return f"WORKFLOW_RUN_ID: {self.wfr_id}, WORKFLOW_TYPE: {self.type_name}, WORKFLOW_START: {self.start}"


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

        subject = kwargs.get('subject', None)
        if subject:
            qs = qs.filter(subject_id__iexact=subject)

        sample = kwargs.get('sample', None)
        if sample:
            qs = qs.filter(sample_id__iexact=sample)

        library = kwargs.get('library', None)
        if library:
            qs = qs.filter(library_id__iexact=library)

        type_ = kwargs.get('type', None)
        if type_:
            qs = qs.filter(type__iexact=type_)

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


class LibraryRun(models.Model):
    # class Meta:
    #     unique_together = ['sequence_run', 'library']

    id = models.BigAutoField(primary_key=True)
    sequence_run = models.ForeignKey(SequenceRun, on_delete=models.SET_NULL, null=True, blank=True)
    library = models.ForeignKey(LabMetadata, on_delete=models.SET_NULL, null=True, blank=True)
    override_cycles = models.CharField(max_length=255)
    coverage = models.CharField(max_length=255)
    qc_pass = models.BooleanField(null=True, default=False)
    status = models.CharField(max_length=255)
    valid = models.BooleanField(null=True, default=True)
