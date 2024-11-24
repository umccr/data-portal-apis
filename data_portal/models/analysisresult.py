from django.db import models
from django.db.models import QuerySet

from data_portal.models import S3Object, GDSFile


class PlatformGeneration(models.IntegerChoices):
    UNKNOWN = 0, 'unknown'
    ONE = 1, 'bcbio'
    TWO = 2, 'icav1'
    THREE = 3, 'icav2'


class AnalysisMethod(models.IntegerChoices):
    UNCLASSIFIED = 0, 'unclassified'
    WGTS = 1, 'wgts'
    SASH = 2, 'sash'
    TSO500 = 3, 'cttso'
    TSO500V2 = 4, 'cttsov2'


class Lookup:

    def __init__(self, key: str, gen: PlatformGeneration = PlatformGeneration.UNKNOWN,
                 method: AnalysisMethod = AnalysisMethod.UNCLASSIFIED):
        self.key: str = key
        self.gen: PlatformGeneration = gen
        self.method: AnalysisMethod = method

    def key(self):
        return self.key

    def gen(self):
        return self.gen

    def method(self):
        return self.method


class AnalysisResultManager(models.Manager):

    def get_by_lookup(self, lookup: Lookup):
        return self.filter(
            key=lookup.key,
            gen=lookup.gen,
            method=lookup.method,
        )

    def create_or_update(self, lookup: Lookup, **kwargs):
        s3objects = kwargs.get('s3objects')
        gdsfiles = kwargs.get('gdsfiles')

        qs: QuerySet = self.get_by_lookup(lookup=lookup)
        if qs.exists():
            result: AnalysisResult = qs.get()
        else:
            result: AnalysisResult = AnalysisResult.objects.create(
                key=lookup.key,
                gen=lookup.gen,
                method=lookup.method,
            )

        if s3objects:
            result.s3objects.add(*s3objects)
        if gdsfiles:
            result.gdsfiles.add(*gdsfiles)
        result.save()


class AnalysisResult(models.Model):
    """
    Centralise to "Key" as lookup value. This is the only string value (varchar 255) to represent business lookup key.
    Using dimensional attributes as IntegerField value is intentional design pattern for building index
    (vectorised/matrix) relation table which will be linked to PK ID for fast retrieval and efficient index maintenance!
    """
    class Meta:
        unique_together = ['key', 'gen', 'method']

    id = models.BigAutoField(primary_key=True)
    key = models.CharField(max_length=255, null=False, blank=False)
    gen = models.IntegerField(choices=PlatformGeneration, null=False, blank=False)
    method = models.IntegerField(choices=AnalysisMethod, null=False, blank=False)

    s3objects = models.ManyToManyField(S3Object)
    gdsfiles = models.ManyToManyField(GDSFile)

    objects = AnalysisResultManager()

    def __str__(self):
        return f"ID: {self.id}, KEY: {self.key}, GEN: {self.gen}, METHOD: {self.method}"
