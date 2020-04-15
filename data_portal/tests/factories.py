import factory
from django.utils.timezone import now

from data_portal.models import S3Object, LIMSRow, S3LIMS, GDSFile


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


class GDSFileFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = GDSFile

    file_id = "fil.feew7airaed6Oon5IeGhoy4queemeequ"
    name = "Test.txt"
    volume_id = "vol.euniehaFahri5eetah0oonohngee1bie"
    volume_name = "umccr-run-data-dev"
    tenant_id = "ookohRahWee0ko1epoon3ej5tezeecu2thaec3AhsaSh3uqueeThasu0guTheeyeecheemoh9tu3neiGh0"
    sub_tenant_id = "wid:f687447b-d13e-4464-a6b8-7167fc75742d"
    path = "/Runs/200401_B00130_0134_GU9AICA8AI/Test.txt"
    time_created = "2020-04-08T02:00:58.026467Z"
    created_by = "14c99f4f-8934-4af2-9df2-729e1b840f42"
    time_modified = "2020-04-01T20:55:35.025Z"
    modified_by = "14c99f4f-8934-4af2-9df2-729e1b840f42"
    inherited_acl = [
        "tid:ookohRahWee0ko1epoon3ej5tezeecu2thaec3AhsaSh3uqueeThasu0guTheeyeecheemoh9tu3neiGh0",
        "wid:cf5c71a5-85c9-4c60-971a-cd1426dbbd5e",
        "wid:58e3d90f-2570-4aeb-a606-bbde78eae677",
        "wid:f687447b-d13e-4464-a6b8-7167fc75742d"
    ]
    urn = "urn:ilmn:iap:aps2:ookohRahWee0ko1epoon3ej5tezeecu2thaec3AhsaSh3uqueeThasu0guTheeyeecheemoh9tu3neiGh0:file" \
          ":fil.feew7airaed6Oon5IeGhoy4queemeequ#/Runs/200401_B00130_0134_GU9AICA8AI/Test.txt "
    size_in_bytes = 1000000000000000
    is_uploaded = True
    archive_status = "None"
    storage_tier = "Standard"
