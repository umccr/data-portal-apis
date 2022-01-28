import logging
from typing import Union

from django.db import models
from django.db.models import QuerySet

from data_portal.models.base import PortalBaseManager, PortalBaseModel
from data_portal.models.s3object import S3Object

logger = logging.getLogger(__name__)


class LIMSRowManager(PortalBaseManager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()

        subject = kwargs.get('subject', None)
        if subject:
            qs = qs.filter(self.reduce_multi_values_qor('subject_id', subject))
            kwargs.pop('subject')

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(self.reduce_multi_values_qor('illumina_id', run))
            kwargs.pop('run')

        return self.get_model_fields_query(qs, **kwargs)


class LIMSRow(PortalBaseModel):
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

    @classmethod
    def get_table_name(cls):
        return cls._meta.db_table

    @classmethod
    def truncate(cls):
        # with connection.cursor() as cursor:
        #     cursor.execute(f"TRUNCATE TABLE {cls.get_table_name()};")
        # TODO we need to get rid of S3LIMS association table -- see below issue #343 deprecation note
        raise NotImplementedError(f"Table truncation for {cls.get_table_name()} is not supported yet.")


class S3LIMS(models.Model):
    """TODO mark to be deprecated, see https://github.com/umccr/data-portal-apis/issues/343
    Models the association between a S3 object and a LIMS row
    """

    class Meta:
        unique_together = ['s3_object', 'lims_row']

    id = models.BigAutoField(primary_key=True)
    s3_object = models.ForeignKey(S3Object, on_delete=models.CASCADE)
    lims_row = models.ForeignKey(LIMSRow, on_delete=models.CASCADE)


class Configuration(models.Model):
    """TODO this might as well be deprecated, not using anywhere
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
