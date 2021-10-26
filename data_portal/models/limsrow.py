import logging

from django.db import models
from django.db.models import QuerySet


logger = logging.getLogger(__name__)

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
