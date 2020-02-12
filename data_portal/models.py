import random
import re
from typing import Union

from django.db import models
from django.db.models import Max, QuerySet

from data_portal.exceptions import RandSamplesTooLarge
from data_portal.fields import HashField


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
        if re.match(r'SBJ[0-9]', subject_id.upper()):
            bucket = kwargs.get('bucket', None)
            if bucket:
                return self.filter(key__icontains=subject_id).exclude(key__contains='.snakemake').filter(bucket=bucket)
            return self.filter(key__icontains=subject_id).exclude(key__contains='.snakemake')
        return self.none()

    def get_by_illumina_id(self, illumina_id: str, **kwargs) -> QuerySet:
        bucket = kwargs.get('bucket', None)
        if bucket:
            return self.filter(key__icontains=illumina_id).filter(bucket=bucket)
        return self.filter(key__icontains=illumina_id)


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
