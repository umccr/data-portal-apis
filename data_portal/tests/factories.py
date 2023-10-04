import json
from datetime import datetime, timezone
from enum import Enum

import factory
from django.utils.timezone import now, make_aware

from data_portal.models.batch import Batch
from data_portal.models.batchrun import BatchRun
from data_portal.models.gdsfile import GDSFile
from data_portal.models.labmetadata import LabMetadata, LabMetadataType
from data_portal.models.labmetadata import LabMetadataPhenotype
from data_portal.models.libraryrun import LibraryRun
from data_portal.models.limsrow import LIMSRow, S3LIMS
from data_portal.models.s3object import S3Object
from data_portal.models.sequence import Sequence, SequenceStatus
from data_portal.models.sequencerun import SequenceRun
from data_portal.models.workflow import Workflow
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus

utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())


class TestConstant(Enum):
    portal_run_id = "20230101abcdefgh"
    portal_run_id2 = "20230102abcdefgh"
    wfr_id = f"wfr.j317paO8zB6yG25Zm6PsgSivJEoq4Ums"
    wfr_id2 = f"wfr.Q5555aO8zB6yG25Zm6PsgSivGwDx_Uaa"
    wfv_id = f"wfv.TKWp7hsFnVTCE8KhfXEurUfTCqSa6zVx"
    wfl_id = f"wfl.Dc4GzACbjhzOf3NbqAYjSmzkE1oWKI9H"
    oncoanalyser_wgs_portal_run_id = "20230911wgsaaaaa"
    umccrise_portal_run_id = "20230103abcdefgh"
    umccrise_wfr_id = f"wfr.umccrisezB6yG25Zm6PsgSivJEoq4Ums"
    umccrise_wfv_id = f"wfv.umccrisenVTCE8KhfXEurUfTCqSa6zVx"
    umccrise_wfl_id = f"wfl.umccrisejhzOf3NbqAYjSmzkE1oWKI9H"
    rnasum_portal_run_id = "20230104abcdefgh"
    rnasum_wfr_id = f"wfr.rnasumzB6yG25Zm6PsgSivJEoq4Ums"
    rnasum_wfv_id = f"wfv.rnasumnVTCE8KhfXEurUfTCqSa6zVx"
    rnasum_wfl_id = f"wfl.rnasumjhzOf3NbqAYjSmzkE1oWKI9H"
    version = "v1"
    instrument_run_id = "200508_A01052_0001_BH5LY7ACGT"
    instrument_run_id2 = "220101_A01052_0002_XR5LY7TGCA"
    sqr_name = instrument_run_id
    run_id = "r.ACGTlKjDgEy099ioQOeOWg"
    run_id2 = "r.GACTlKjDgEy099io_0000"
    override_cycles = "Y151;I8;I8;Y151"
    subject_id = "SBJ00001"
    library_id_normal = "L2100001"
    lane_normal_library = 1
    library_id_tumor = "L2100002"
    lane_tumor_library = 3
    sample_id = "PRJ210001"
    sample_name_normal = f"{sample_id}_{library_id_normal}"
    sample_name_tumor = f"{sample_id}_{library_id_tumor}"
    wts_library_id_tumor = "L2100003"
    wts_library_id_tumor2 = "L2200001"
    wts_lane_tumor_library = 4
    wts_sample_id = "MDX210002"


class LabMetadataFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LabMetadata

    library_id = TestConstant.library_id_normal.value
    sample_name = "Ambiguous Sample"
    sample_id = TestConstant.sample_id.value
    external_sample_id = "DNA123456"
    subject_id = TestConstant.subject_id.value
    external_subject_id = "PM1234567"
    phenotype = LabMetadataPhenotype.NORMAL.value
    quality = "good"
    source = "blood"
    project_name = "CUP"
    project_owner = "UMCCR"
    experiment_id = "TSqN123456LL"
    type = "WGS"
    assay = "TsqNano"
    override_cycles = TestConstant.override_cycles.value
    workflow = "clinical"
    coverage = "40.0"
    truseqindex = "A09"


class TumorLabMetadataFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LabMetadata

    library_id = TestConstant.library_id_tumor.value
    sample_name = "Ambiguous Sample"
    sample_id = TestConstant.sample_id.value
    external_sample_id = "DNA123456"
    subject_id = TestConstant.subject_id.value
    external_subject_id = "PM1234567"
    phenotype = LabMetadataPhenotype.TUMOR.value
    quality = "good"
    source = "blood"
    project_name = "CUP"
    project_owner = "UMCCR"
    experiment_id = "TSqN123456LL"
    type = "WGS"
    assay = "TsqNano"
    override_cycles = TestConstant.override_cycles.value
    workflow = "clinical"
    coverage = "40.0"
    truseqindex = "A09"


class WtsTumorLabMetadataFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LabMetadata

    library_id = TestConstant.wts_library_id_tumor.value
    sample_name = "Ambiguous WTS Sample"
    sample_id = TestConstant.wts_sample_id.value
    external_sample_id = "RNA123456"
    subject_id = TestConstant.subject_id.value
    external_subject_id = "PM1234567"
    phenotype = LabMetadataPhenotype.TUMOR.value
    quality = "good"
    source = "blood"
    project_name = "CUP"
    project_owner = "UMCCR"
    experiment_id = "TSqN123456LL"
    type = LabMetadataType.WTS.value
    assay = "NebRNA"
    override_cycles = TestConstant.override_cycles.value
    workflow = "clinical"
    coverage = "40.0"
    truseqindex = "A09"


class WtsTumorLabMetadataFactory2(factory.django.DjangoModelFactory):
    class Meta:
        model = LabMetadata

    library_id = TestConstant.wts_library_id_tumor2.value
    sample_name = "Ambiguous WTS Sample 2"
    sample_id = TestConstant.wts_sample_id.value
    external_sample_id = "RNA123456"
    subject_id = TestConstant.subject_id.value
    external_subject_id = "PM1234567"
    phenotype = LabMetadataPhenotype.TUMOR.value
    quality = "good"
    source = "blood"
    project_name = "CUP"
    project_owner = "UMCCR"
    experiment_id = "TSqN123456LL"
    type = LabMetadataType.WTS.value
    assay = "NebRNA"
    override_cycles = TestConstant.override_cycles.value
    workflow = "clinical"
    coverage = "40.0"
    truseqindex = "A09"


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


class LibraryRunFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LibraryRun

    library_id = TestConstant.library_id_normal.value
    instrument_run_id = TestConstant.instrument_run_id.value
    run_id = TestConstant.run_id.value
    lane = TestConstant.lane_normal_library.value
    override_cycles = TestConstant.override_cycles.value


class TumorLibraryRunFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LibraryRun

    library_id = TestConstant.library_id_tumor.value
    instrument_run_id = TestConstant.instrument_run_id.value
    run_id = TestConstant.run_id.value
    lane = TestConstant.lane_tumor_library.value
    override_cycles = TestConstant.override_cycles.value


class WtsTumorLibraryRunFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LibraryRun

    library_id = TestConstant.wts_library_id_tumor.value
    instrument_run_id = TestConstant.instrument_run_id.value
    run_id = TestConstant.run_id.value
    lane = TestConstant.wts_lane_tumor_library.value
    override_cycles = TestConstant.override_cycles.value


class WtsTumorLibraryRunFactory2(factory.django.DjangoModelFactory):
    class Meta:
        model = LibraryRun

    library_id = TestConstant.wts_library_id_tumor2.value
    instrument_run_id = TestConstant.instrument_run_id2.value
    run_id = TestConstant.run_id2.value
    lane = TestConstant.wts_lane_tumor_library.value
    override_cycles = TestConstant.override_cycles.value


class SequenceFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Sequence

    instrument_run_id = TestConstant.sqr_name.value
    run_id = "r.ACGTlKjDgEy099ioQOeOWg"
    sample_sheet_name = "SampleSheet.csv"
    gds_folder_path = f"/Runs/{instrument_run_id}_{run_id}"
    gds_volume_name = "bssh.acgtacgt498038ed99fa94fe79523959"
    reagent_barcode = "NV9999999-RGSBS"
    flowcell_barcode = "BARCODEEE"
    status = SequenceStatus.STARTED
    start_time = make_aware(datetime.utcnow())
    end_time = None


class SequenceRunFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = SequenceRun

    run_id = TestConstant.run_id.value
    date_modified = make_aware(datetime.utcnow())
    status = "PendingAnalysis"
    instrument_run_id = TestConstant.instrument_run_id.value
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


class SequenceRunFactory2(factory.django.DjangoModelFactory):
    class Meta:
        model = SequenceRun

    run_id = TestConstant.run_id2.value
    date_modified = make_aware(datetime.utcnow())
    status = "PendingAnalysis"
    instrument_run_id = TestConstant.instrument_run_id2.value
    gds_folder_path = f"/Runs/{instrument_run_id}_{run_id}"
    gds_volume_name = "bssh.acgtacgt498038ed99fa94fe79523959"
    reagent_barcode = "RV9999999-SSSSS"
    v1pre3_id = "999999"
    acl = ["wid:acgtacgt-9999-38ed-99fa-94fe79523959"]
    flowcell_barcode = "BARCODEEE"
    sample_sheet_name = "SampleSheet.csv"
    api_url = f"https://api.aps2.sh.basespace.illumina.com/v2/runs/{run_id}"
    name = instrument_run_id
    msg_attr_action = "statuschanged"
    msg_attr_action_type = "bssh.runs"
    msg_attr_action_date = "2022-10-09T22:17:10.815Z"
    msg_attr_produced_by = "BaseSpaceSequenceHub"


class WorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    sequence_run = factory.SubFactory(SequenceRunFactory)
    portal_run_id = TestConstant.portal_run_id.value
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.BCL_CONVERT.value
    input = json.dumps({
        "mock": "must load template from ssm parameter store"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{w.sequence_run.name}__{w.sequence_run.run_id}__{utc_now_ts}"
    )


class OncoanalyserWgsWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.oncoanalyser_wgs_portal_run_id.value
    type_name = WorkflowType.ONCOANALYSER_WGS.value
    input = json.dumps({
        "mock": "must override me, after factory init call"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value


class DragenWgsQcWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    sequence_run = factory.SubFactory(SequenceRunFactory)
    portal_run_id = TestConstant.portal_run_id.value
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.DRAGEN_WGS_QC.value
    input = json.dumps({
        "mock": "must load template from ssm parameter store"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{w.sequence_run.name}__{w.sequence_run.run_id}__{utc_now_ts}"
    )


class DragenWtsQcWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    sequence_run = factory.SubFactory(SequenceRunFactory)
    portal_run_id = TestConstant.portal_run_id.value
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.DRAGEN_WTS_QC.value
    input = json.dumps({
        "mock": "must load template from ssm parameter store"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{w.sequence_run.name}__{w.sequence_run.run_id}__{utc_now_ts}"
    )


class DragenWtsWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.portal_run_id.value
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.DRAGEN_WTS.value
    input = json.dumps({
        "mock": "override me"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{utc_now_ts}"
    )


class DragenWtsWorkflowFactory2(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.portal_run_id2.value
    wfr_id = TestConstant.wfr_id2.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.DRAGEN_WTS.value
    input = json.dumps({
        "mock": "override me"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{utc_now_ts}"
    )


class RNAsumWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.rnasum_portal_run_id.value
    wfr_id = TestConstant.rnasum_wfr_id.value
    wfv_id = TestConstant.rnasum_wfv_id.value
    wfl_id = TestConstant.rnasum_wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.RNASUM.value
    input = json.dumps({
        "mock": "override me"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{utc_now_ts}"
    )


class TumorNormalWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.portal_run_id.value
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.TUMOR_NORMAL.value
    input = json.dumps({
        "mock": "override me"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{utc_now_ts}"
    )


class UmccriseWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.umccrise_portal_run_id.value
    wfr_id = TestConstant.umccrise_wfr_id.value
    wfv_id = TestConstant.umccrise_wfv_id.value
    wfl_id = TestConstant.umccrise_wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.UMCCRISE.value
    input = json.dumps({
        "mock": "override me"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{utc_now_ts}"
    )


class StarAlignmentWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    portal_run_id = TestConstant.portal_run_id.value
    type_name = WorkflowType.STAR_ALIGNMENT.value
    input = json.dumps({
        "mock": "override me"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{utc_now_ts}"
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
    step = WorkflowType.DRAGEN_WGS_QC.value
    running = True
    notified = True


class ReportLinkedS3ObjectFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = S3Object

    bucket = 'some-bucket'
    key = factory.Sequence(lambda n: 'cancer_report_tables/SBJ00001__SBJ00001_MDX000001_L0000001_%d.json.gz' % n)
    size = 1000
    last_modified_date = now()
    e_tag = 'etag'
