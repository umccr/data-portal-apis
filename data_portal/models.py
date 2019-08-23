from typing import Union

from django.db import models


class S3Object(models.Model):
    class Meta:
        unique_together = ['bucket', 'key']

    bucket = models.CharField(max_length=255)
    key = models.CharField(max_length=255)
    size = models.IntegerField()
    last_modified_date = models.DateTimeField()
    e_tag = models.CharField(max_length=255)


class LIMSRow(models.Model):
    illumina_id = models.CharField(max_length=255)
    run = models.IntegerField()
    timestamp = models.DateField()
    sample_id = models.CharField(max_length=255)
    sample_name = models.CharField(max_length=255)
    project = models.CharField(max_length=255)
    subject_id = models.CharField(max_length=255)
    type = models.CharField(max_length=255)
    phenotype = models.CharField(max_length=255)
    source = models.CharField(max_length=255)
    quality = models.CharField(max_length=255)
    secondary_analysis = models.CharField(max_length=255)
    fastq = models.CharField(max_length=255)
    number_fastqs = models.CharField(max_length=255)
    results = models.CharField(max_length=255)

    def __str__(self):
        return 'rn=%s, illumina_id=%s, sample_id=%s, sample_name=%s, subject_id=%s' \
               % (self.id, self.illumina_id, self.sample_id, self.sample_name, self.subject_id)


class S3LIMS(models.Model):
    class Meta:
        unique_together = ['s3_object', 'lims_row']

    s3_object = models.ForeignKey(S3Object, on_delete=models.CASCADE)
    lims_row = models.ForeignKey(LIMSRow, on_delete=models.CASCADE)


class Configuration(models.Model):
    LAST_LIMS_DATA_ETAG = 'LAST_LIMS_DATA_ETAG'

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
