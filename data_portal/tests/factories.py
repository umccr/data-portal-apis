import json
from datetime import datetime, timezone
from enum import Enum

import factory
from django.utils.timezone import now, make_aware

from data_portal.models import S3Object, LIMSRow, S3LIMS, GDSFile, SequenceRun, Workflow, Batch, BatchRun, Report
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus

utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())


class TestConstant(Enum):
    wfr_id = f"wfr.j317paO8zB6yG25Zm6PsgSivJEoq4Ums"
    wfv_id = f"wfv.TKWp7hsFnVTCE8KhfXEurUfTCqSa6zVx"
    wfl_id = f"wfl.Dc4GzACbjhzOf3NbqAYjSmzkE1oWKI9H"
    version = "v1"
    sqr_name = "200508_A01052_0001_BH5LY7ACGT"


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


class SequenceRunFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = SequenceRun

    run_id = "r.ACGTlKjDgEy099ioQOeOWg"
    date_modified = make_aware(datetime.utcnow())
    status = "PendingAnalysis"
    instrument_run_id = TestConstant.sqr_name.value
    gds_folder_path = f"/Runs/{instrument_run_id}_{run_id}"
    gds_volume_name = "bssh.acgtacgt498038ed99fa94fe79523959"
    reagent_barcode = "NV9999999-RGSBS"
    v1pre3_id = "666666"
    acl = ["wid:acgtacgt-9999-38ed-99fa-94fe79523959"]
    flowcell_barcode = "BARCODEEE"
    sample_sheet_name = "SampleSheet.csv"
    api_url = f"https://api.aps2.sh.basespace.illumina.com/v2/runs/{run_id}"
    name = instrument_run_id
    msg_attr_action = "statuschanged"
    msg_attr_action_type = "bssh.runs"
    msg_attr_action_date = "2020-05-09T22:17:10.815Z"
    msg_attr_produced_by = "BaseSpaceSequenceHub"


class WorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    sequence_run = factory.SubFactory(SequenceRunFactory)
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.BCL_CONVERT.name
    input = json.dumps({
        "mock": "must load template from ssm parameter store"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{w.sequence_run.name}__{w.sequence_run.run_id}__{utc_now_ts}"
    )


class BatchFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Batch

    name = TestConstant.sqr_name.value
    created_by = TestConstant.wfr_id.value
    context_data = None


class BatchRunFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = BatchRun

    batch = factory.SubFactory(BatchFactory)
    step = WorkflowType.GERMLINE.name
    running = True
    notified = True


class ReportsFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Report

