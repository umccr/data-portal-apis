import factory
from django.utils.timezone import now

from data_portal.models import S3Object, LIMSRow, S3LIMS


class S3ObjectFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = S3Object

    bucket = 'some-bucket'
    key = factory.Sequence(lambda n: 'key-%d.csv' % n)
    size = 1000
    last_modified_date = now()
    e_tag = 'etag'


class LIMSRowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LIMSRow

    illumina_id = factory.Sequence(lambda n: 'illumina_%d' % n)
    run = 1
    timestamp = now().date()
    subject_id = "subject_id"
    sample_id = "sample_id"
    library_id = factory.Sequence(lambda n: 'library_id_%d' % n)
    external_subject_id = "external_subject_id"
    external_sample_id = "external_sample_id"
    external_library_id = "external_library_id"
    sample_name = "sample_name"
    project_owner = "project_owner"
    project_name = "project_name"
    type = "type"
    assay = "assay"
    phenotype = "phenotype"
    source = "source"
    quality = "quality"
    topup = "topup"
    secondary_analysis = "secondary_analysis"
    fastq = "fastq"
    number_fastqs = "number_fastqs"
    results = "results"
    trello = "trello"
    notes = "some_phenotype"
    todo = "todo"


class S3LIMSFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = S3LIMS

    s3_object = factory.SubFactory(S3ObjectFactory)
    lims_row = factory.SubFactory(LIMSRowFactory)
