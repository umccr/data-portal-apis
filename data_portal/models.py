import random
from typing import Union

from django.db import models
from django.db.models import Max, QuerySet, Q

from data_portal.exceptions import RandSamplesTooLarge
from data_portal.fields import HashField
from data_processors.pipeline.constant import WorkflowStatus


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

    def get_all(self):
        return self.exclude(key__contains='.snakemake')

    def get_by_subject_id(self, subject_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(key__icontains=subject_id).exclude(key__contains='.snakemake')
        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
        return qs

    def get_subject_results(self, subject_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(key__icontains=subject_id).exclude(key__contains='.snakemake')
        bam = Q(key__iregex='wgs') & Q(key__iregex='ready') & Q(key__iregex='.bam$')
        vcf = (Q(key__iregex='umccrised/[^(work)*]') & Q(key__iregex='small_variants/[^\/]*(.vcf.gz$|.maf$)'))
        cancer = Q(key__iregex='umccrised') & Q(key__iregex='cancer_report.html$')
        qc = Q(key__iregex='umccrised') & Q(key__iregex='multiqc_report.html$')
        pcgr = Q(key__iregex='umccrised/[^\/]*/[^\/]*(pcgr|cpsr).html$')
        coverage = Q(key__iregex='cacao') & Q(key__iregex='html') & Q(key__iregex='(cacao_normal|cacao_tumor)')
        circos = (Q(key__iregex='work/') & Q(key__iregex='purple/') & Q(key__iregex='circos')
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

    def get_by_illumina_id(self, illumina_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(key__icontains=illumina_id)
        bucket = kwargs.get('bucket', None)
        if bucket:
            qs = qs.filter(bucket=bucket)
        return qs


class S3Object(models.Model):
    """
    MySQL has character length limitation on unique indexes and since key column require to store
    lengthy path, unique_hash is sha256sum of bucket and key for unique indexes purpose.
    """

    bucket = models.CharField(max_length=255)
    key = models.TextField()
    size = models.BigIntegerField()
    last_modified_date = models.DateTimeField()
    e_tag = models.CharField(max_length=255)
    unique_hash = HashField(unique=True, base_fields=['bucket', 'key'], default=None)

    SORTABLE_COLUMNS = ['size', 'last_modified_date']
    DEFAULT_SORT_COL = 'last_modified_date'

    objects = S3ObjectManager()


class LIMSRow(models.Model):
    """
    Models a row in the LIMS data. Fields are the columns.
    """

    class Meta:
        unique_together = ['illumina_id', 'library_id']

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
    phenotype = models.CharField(max_length=255, null=True, blank=True)
    source = models.CharField(max_length=255, null=True, blank=True)
    quality = models.CharField(max_length=255, null=True, blank=True)
    topup = models.CharField(max_length=255, null=True, blank=True)
    secondary_analysis = models.CharField(max_length=255, null=True, blank=True)
    fastq = models.TextField(null=True, blank=True)
    number_fastqs = models.CharField(max_length=255, null=True, blank=True)
    results = models.TextField(null=True, blank=True)
    trello = models.TextField(null=True, blank=True)
    notes = models.TextField(null=True, blank=True)
    todo = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return 'id=%s, illumina_id=%s, sample_id=%s, sample_name=%s, subject_id=%s' \
               % (self.id, self.illumina_id, self.sample_id, self.sample_name, self.subject_id)

    # The attributes used to find linking between LIMSRow and S3Object(Key)
    # In short, these attr values should be part of the S3 Object key.
    # This is the common logic used for both persisting s3 and lims.
    S3_LINK_ATTRS = ('subject_id', 'sample_id')


class S3LIMS(models.Model):
    """
    Models the association between a S3 object and a LIMS row
    """

    class Meta:
        unique_together = ['s3_object', 'lims_row']

    s3_object = models.ForeignKey(S3Object, on_delete=models.CASCADE)
    lims_row = models.ForeignKey(LIMSRow, on_delete=models.CASCADE)


class Configuration(models.Model):
    """
    Model that stores a configuration value
    Currently not used; but could be used for cases when we want to record the state of some data - e.g. LIMS file.
    """
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

    def get_all(self):
        return self.exclude(path__contains='.snakemake')

    def get_by_subject_id(self, subject_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(path__icontains=subject_id).exclude(path__contains='.snakemake')
        volume_name = kwargs.get('volume_name', None)
        if volume_name:
            qs = qs.filter(volume_name=volume_name)
        volume_id = kwargs.get('volume_id', None)
        if volume_id:
            qs = qs.filter(volume_name=volume_id)
        return qs

    def get_by_illumina_id(self, illumina_id: str, **kwargs) -> QuerySet:
        qs: QuerySet = self.filter(path__icontains=illumina_id)
        volume_name = kwargs.get('volume_name', None)
        if volume_name:
            qs = qs.filter(volume_name=volume_name)
        volume_id = kwargs.get('volume_id', None)
        if volume_id:
            qs = qs.filter(volume_name=volume_id)
        return qs


class GDSFile(models.Model):
    """
    Model wrap around IAP GDS File - GET /v1/files/{fileId}
    https://aps2.platform.illumina.com/gds/swagger/index.html

    NOTE:
    For composite (unique) key, it follows S3 style bucket + key pattern, see unique_hash.
    i.e. gds://volume_name/path ~ s3://bucket/key and, this full path must be unique globally.
    """
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


class SequenceRun(models.Model):
    class Meta:
        unique_together = ['run_id', 'date_modified', 'status']

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

    def __str__(self):
        return f"Run ID '{self.run_id}', " \
               f"Instrument ID '{self.instrument_run_id}', " \
               f"Date Modified '{self.date_modified}', " \
               f"Status '{self.status}'"


class Batch(models.Model):
    class Meta:
        unique_together = ['name', 'created_by']

    name = models.CharField(max_length=255)
    created_by = models.CharField(max_length=255)
    context_data = models.TextField(null=True, blank=True)

    def __str__(self):
        return f"ID: {self.id}, NAME: {self.name}, CREATED_BY: {self.created_by}"


class BatchRun(models.Model):
    batch = models.ForeignKey(Batch, on_delete=models.SET_NULL, null=True, blank=True)
    step = models.CharField(max_length=255)
    running = models.BooleanField(null=True, blank=True)

    def __str__(self):
        return f"ID: {self.id}, STEP: {self.step}, RUNNING: {self.running}, BATCH_ID: {self.batch.id}"


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
            end_status__isnull=True,
            start__isnull=False,
            input__isnull=False,
        )
        return qs


class Workflow(models.Model):
    class Meta:
        unique_together = ['wfr_id', 'wfl_id', 'wfv_id']

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
