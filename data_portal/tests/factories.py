import json
from datetime import datetime, timezone
from enum import Enum

import factory
from django.utils.timezone import now, make_aware

from data_portal.models import S3Object, LIMSRow, S3LIMS, GDSFile, SequenceRun, Workflow, Batch, BatchRun, \
    Report, ReportType, LabMetadata, Sequence, SequenceStatus, LibraryRun, LabMetadataPhenotype
from data_processors.pipeline.domain.workflow import WorkflowType, WorkflowStatus

utc_now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())


class TestConstant(Enum):
    wfr_id = f"wfr.j317paO8zB6yG25Zm6PsgSivJEoq4Ums"
    wfv_id = f"wfv.TKWp7hsFnVTCE8KhfXEurUfTCqSa6zVx"
    wfl_id = f"wfl.Dc4GzACbjhzOf3NbqAYjSmzkE1oWKI9H"
    version = "v1"
    instrument_run_id = "200508_A01052_0001_BH5LY7ACGT"
    sqr_name = instrument_run_id
    run_id = "r.ACGTlKjDgEy099ioQOeOWg"
    override_cycles = "Y151;I8;I8;Y151"
    subject_id = "SBJ00001"
    library_id_normal = "L2100001"
    lane_normal_library = 1
    library_id_tumor = "L2100002"
    lane_tumor_library = 3
    sample_id = "PRJ210001"
    sample_name_normal = f"{sample_id}_{library_id_normal}"
    sample_name_tumor = f"{sample_id}_{library_id_tumor}"


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


class DragenWgsQcWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    sequence_run = factory.SubFactory(SequenceRunFactory)
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.DRAGEN_WGS_QC.name
    input = json.dumps({
        "mock": "must load template from ssm parameter store"
    })
    start = make_aware(datetime.now())
    end_status = WorkflowStatus.RUNNING.value
    notified = True

    wfr_name = factory.LazyAttribute(
        lambda w: f"umccr__{w.type_name}__{w.sequence_run.name}__{w.sequence_run.run_id}__{utc_now_ts}"
    )


class TumorNormalWorkflowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workflow

    sequence_run = factory.SubFactory(SequenceRunFactory)
    wfr_id = TestConstant.wfr_id.value
    wfv_id = TestConstant.wfv_id.value
    wfl_id = TestConstant.wfl_id.value
    version = TestConstant.version.value
    type_name = WorkflowType.TUMOR_NORMAL.name
    input = json.dumps({
        "mock": "override me"
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
    step = WorkflowType.DRAGEN_WGS_QC.name
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


class ReportFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Report

    subject_id = "SBJ00001"
    sample_id = "MDX000001"
    library_id = "L0000001"
    created_by = "cancer_report_tables"

    type = "unknown"
    data = None
    s3_object_id = None
    gds_file_id = None


class FusionCallerMetricsReportFactory(ReportFactory):
    type = ReportType.FUSION_CALLER_METRICS
    report_uri = "gds://development/analysis_data/SBJ00001/dragen_tso_ctdna/2021-08-26__05-39-57/Results/PRJ000001_L0000001/PRJ000001_L0000001.AlignCollapseFusionCaller_metrics.json.gz"

    data = {
        "MappingAligningPerRg": [
            {
                "name": "Total reads in RG",
                "value": 1386181288.0,
                "percent": 100.0
            }
        ],
        "MappingAligningSummary": [
            {
                "name": "Total input reads",
                "value": 1386181288.0,
                "percent": 100.0
            },
            {
                "name": "DRAGEN mapping rate [mil. reads/second]",
                "value": 1.06
            }
        ],
        "TrimmerStatistics": [
            {
                "name": "Total input reads",
                "value": 210446120.0
            },
            {
                "name": "Total input bases",
                "value": 29260431093.0
            }
        ],
        "CoverageSummary": [
            {
                "name": "Aligned bases",
                "value": 28298345615.0
            },
            {
                "name": "Aligned bases in genome",
                "value": 28298345615.0,
                "percent": 100.0
            }
        ],
        "SvSummary": [
            {
                "name": "Number of deletions (PASS)",
                "value": 1225.0
            },
            {
                "name": "Number of insertions (PASS)",
                "value": 0.0
            },
            {
                "name": "Number of duplications (PASS)",
                "value": 1270.0
            },
            {
                "name": "Number of breakend pairs (PASS)",
                "value": 91418.0
            }
        ],
        "RunTime": [
            {
                "name": "Time loading reference",
                "value": "00:01:28.736",
                "percent": 88.74
            },
            {
                "name": "Time aligning reads",
                "value": "00:21:35.228",
                "percent": 1295.23
            },
            {
                "name": "Time UMI read collapsing and remapping",
                "value": "01:28:10.404",
                "percent": 5290.4
            },
            {
                "name": "Time sorting",
                "value": "00:01:22.266",
                "percent": 82.27
            },
            {
                "name": "Time saving map/align output",
                "value": "00:01:48.219",
                "percent": 108.22
            },
            {
                "name": "Time partial reconfiguration",
                "value": "00:00:01.669",
                "percent": 1.67
            },
            {
                "name": "Time partitioning",
                "value": "01:49:44.656",
                "percent": 6584.66
            },
            {
                "name": "Time structural variant calling",
                "value": "02:02:06.935",
                "percent": 7326.94
            },
            {
                "name": "Total runtime",
                "value": "03:55:40.646",
                "percent": 14140.65
            }
        ]
    }


class HRDChordReportFactory(ReportFactory):
    type = ReportType.HRD_CHORD

    data = [
        {
            "sample": "SBJ00001_MDX000001_L0000001",
            "p_hrd": 0,
            "hr_status": "HR_proficient",
            "hrd_type": "none",
            "p_BRCA1": 0,
            "p_BRCA2": 0,
            "remarks_hr_status": "",
            "remarks_hrd_type": "",
            "p_hrd.5%": 0,
            "p_hrd.50%": 0,
            "p_hrd.95%": 0.004,
            "p_BRCA1.5%": 0,
            "p_BRCA1.50%": 0,
            "p_BRCA1.95%": 0,
            "p_BRCA2.5%": 0,
            "p_BRCA2.50%": 0,
            "p_BRCA2.95%": 0.004
        }
    ]


class HRDetectReportFactory(ReportFactory):
    type = ReportType.HRD_HRDETECT

    data = [
        {
            "sample": "SBJ00001_MDX000001_L0000001",
            "Probability": 0.001,
            "intercept": -3.364,
            "del.mh.prop": 0.38,
            "SNV3": -0.95,
            "SV3": -0.877,
            "SV5": -1.105,
            "hrdloh_index": -0.719,
            "SNV8": 0.039
        }
    ]


class PurpleCNVGermReportFactory(ReportFactory):
    type = ReportType.PURPLE_CNV_GERM

    data = [
        {
            "Chr": "chr1",
            "Start": 24194001,
            "End": 24197000,
            "CN": 0,
            "CN Min+Maj": "0+0",
            "Start/End SegSupport": "NONE-NONE",
            "Method": "GERMLINE_HOM_DELETION",
            "BAF (count)": "1 (0)",
            "GC (windowCount)": "0.47 (1)"
        },
        {
            "Chr": "chr1",
            "Start": 61617001,
            "End": 61618000,
            "CN": 0.1,
            "CN Min+Maj": "0.1+0.1",
            "Start/End SegSupport": "NONE-NONE",
            "Method": "GERMLINE_HET2HOM_DELETION",
            "BAF (count)": "0.55 (0)",
            "GC (windowCount)": "0.39 (1)"
        },
    ]


class PurpleCNVSomReportFactory(ReportFactory):
    type = ReportType.PURPLE_CNV_SOM

    data = [
        {
            "Chr": "chr1",
            "Start": 1,
            "End": 7196377,
            "CN": 0.7,
            "CN Min+Maj": "0+0.7",
            "Start/End SegSupport": "TELOMERE-BND",
            "Method": "BAF_WEIGHTED",
            "BAF (count)": "1 (6)",
            "GC (windowCount)": "0.52 (4515)"
        },
        {
            "Chr": "chr1",
            "Start": 7196378,
            "End": 7295500,
            "CN": 1,
            "CN Min+Maj": "0.3+0.7",
            "Start/End SegSupport": "BND-BND",
            "Method": "BAF_WEIGHTED",
            "BAF (count)": "0.71 (0)",
            "GC (windowCount)": "0.45 (94)"
        },
    ]


class PurpleCNVSomGeneReportFactory(ReportFactory):
    type = ReportType.PURPLE_CNV_SOM_GENE

    data = [
        {
            "gene": "GNB1",
            "minCN": 0.7234,
            "maxCN": 0.7234,
            "chrom": "chr1",
            "start": 1785285,
            "end": 1891117,
            "chrBand": "p36.33",
            "onco_or_ts": "",
            "transcriptID": "ENST00000610897.4",
            "minMinorAlleleCN": 0,
            "somReg": 1,
            "germDelReg": "0/0",
            "minReg": 1,
            "minRegStartEnd": "1-7196377",
            "minRegSupportStartEndMethod": "TELOMERE-BND (BAF_WEIGHTED)"
        },
        {
            "gene": "SKI",
            "minCN": 0.7234,
            "maxCN": 0.7234,
            "chrom": "chr1",
            "start": 2228695,
            "end": 2310119,
            "chrBand": "p36.32-p36.33",
            "onco_or_ts": "oncogene",
            "transcriptID": "ENST00000378536.4",
            "minMinorAlleleCN": 0,
            "somReg": 1,
            "germDelReg": "0/0",
            "minReg": 1,
            "minRegStartEnd": "1-7196377",
            "minRegSupportStartEndMethod": "TELOMERE-BND (BAF_WEIGHTED)"
        },
    ]


class SigsDBSReportFactory(ReportFactory):
    type = ReportType.SIGS_DBS

    data = [
        {
            "Rank": 1,
            "Signature": "DBS4",
            "Contribution": 4,
            "RelFreq": 0.33
        },
        {
            "Rank": 2,
            "Signature": "DBS2",
            "Contribution": 3,
            "RelFreq": 0.25
        },
        {
            "Rank": 2,
            "Signature": "DBS3",
            "Contribution": 3,
            "RelFreq": 0.25
        },
        {
            "Rank": 4,
            "Signature": "DBS1",
            "Contribution": 2,
            "RelFreq": 0.17
        },
        {
            "Rank": 5,
            "Signature": "DBS7",
            "Contribution": 0,
            "RelFreq": 0
        }
    ]


class SigsIndelReportFactory(ReportFactory):
    type = ReportType.SIGS_INDEL

    data = [
        {
            "Rank": 1,
            "Signature": "ID1",
            "Contribution": 71,
            "RelFreq": 0.26
        },
        {
            "Rank": 2,
            "Signature": "ID3",
            "Contribution": 34,
            "RelFreq": 0.12
        },
        {
            "Rank": 3,
            "Signature": "ID17",
            "Contribution": 23,
            "RelFreq": 0.08
        },
        {
            "Rank": 4,
            "Signature": "ID14",
            "Contribution": 22,
            "RelFreq": 0.08
        },
        {
            "Rank": 5,
            "Signature": "ID8",
            "Contribution": 19,
            "RelFreq": 0.07
        },
    ]


class SigsSNV2015ReportFactory(ReportFactory):
    type = ReportType.SIGS_SNV_2015

    data = [
        {
            "Rank": 1,
            "Signature": "Sig16",
            "Contribution": 698,
            "RelFreq": 0.3
        },
        {
            "Rank": 2,
            "Signature": "Sig5",
            "Contribution": 401,
            "RelFreq": 0.17
        },
        {
            "Rank": 3,
            "Signature": "Sig8",
            "Contribution": 337,
            "RelFreq": 0.14
        },
        {
            "Rank": 4,
            "Signature": "Sig1",
            "Contribution": 301,
            "RelFreq": 0.13
        },
        {
            "Rank": 5,
            "Signature": "Sig9",
            "Contribution": 239,
            "RelFreq": 0.1
        },
    ]


class SigsSNV2020ReportFactory(ReportFactory):
    type = ReportType.SIGS_SNV_2020

    data = [
        {
            "Rank": 1,
            "Signature": "SBS5",
            "Contribution": 712,
            "RelFreq": 0.3
        },
        {
            "Rank": 2,
            "Signature": "SBS9",
            "Contribution": 194,
            "RelFreq": 0.08
        },
        {
            "Rank": 3,
            "Signature": "SBS40",
            "Contribution": 181,
            "RelFreq": 0.08
        },
        {
            "Rank": 4,
            "Signature": "SBS1",
            "Contribution": 158,
            "RelFreq": 0.07
        },
        {
            "Rank": 5,
            "Signature": "SBS8",
            "Contribution": 155,
            "RelFreq": 0.07
        },
    ]


class SvUnmeltedReportFactory(ReportFactory):
    type = ReportType.SV_UNMELTED

    data = [
        {
            "vcfnum": "001",
            "nann": 1,
            "TierTop": "2",
            "Start": "1:7,196,381",
            "End": "10:80,720,903",
            "Type": "BND",
            "BND_ID": "100",
            "BND_mate": "A",
            "SR_alt": 2,
            "PR_alt": 2,
            "SR_PR_sum": 4,
            "SR_PR_ref": "169,124",
            "AF_BPI": "0.02 (0.02, 0.02)",
            "SScore": 30,
            "annotation": "G]CHR10:80720907]|transcript_ablation|CAMTA1|ENST00000303635_exon_4/22|key_tsgene|2"
        },
        {
            "vcfnum": "002",
            "nann": 2,
            "TierTop": "2",
            "Start": "1:7,295,500",
            "End": "3:171,735,354",
            "Type": "BND",
            "BND_ID": "097",
            "BND_mate": "A",
            "SR_alt": 2,
            "PR_alt": 2,
            "SR_PR_sum": 4,
            "SR_PR_ref": "188,153",
            "AF_BPI": "0.01 (0.01, 0.01)",
            "SScore": 30,
            "annotation": "[CHR3:171735355[A|frameshift_variant&gene_fusion|PLD1&CAMTA1|ENST00000303635_exon_5/22|key_gene|2,[CHR3:171735355[A|gene_fusion|PLD1&CAMTA1|ENST00000303635_exon_5/22|key_gene|2"
        },
    ]


class SvMeltedReportFactory(ReportFactory):
    type = ReportType.SV_MELTED

    data = [
        {
            "vcfnum": "058",
            "nann": 2,
            "TierTop": "1",
            "Start": "2:211,842,747",
            "End": "2:214,067,957",
            "Type": "BND",
            "BND_ID": "092",
            "BND_mate": "B",
            "SR_alt": 2,
            "PR_alt": 2,
            "SR_PR_sum": 4,
            "SR_PR_ref": "114,86",
            "AF_BPI": "0.02 (0.02, 0.02)",
            "SScore": 30,
            "Event": "A]CHR2:214067963]",
            "Effect": "FrameshiftV, FusG",
            "Genes": "ERBB4, SPAG16",
            "Transcript": "ENST00000342788_exon_3/27",
            "Detail": "known_promiscuous",
            "Tier": "1",
            "ntrx": 1,
            "ngen": 2,
            "neff": 2
        },
        {
            "vcfnum": "062",
            "nann": 1,
            "TierTop": "1",
            "Start": "2:214,067,957",
            "End": "2:211,842,747",
            "Type": "BND",
            "BND_ID": "092",
            "BND_mate": "A",
            "SR_alt": 2,
            "PR_alt": 2,
            "SR_PR_sum": 4,
            "SR_PR_ref": "114,86",
            "AF_BPI": "0.02 (0.02, 0.02)",
            "SScore": 30,
            "Event": "C]CHR2:211842747]",
            "Effect": "FrameshiftV, FusG",
            "Genes": "ERBB4, SPAG16",
            "Transcript": "ENST00000331683_exon_13/15",
            "Detail": "known_promiscuous",
            "Tier": "1",
            "ntrx": 1,
            "ngen": 2,
            "neff": 2
        },
    ]


class SvBNDMainReportFactory(ReportFactory):
    type = ReportType.SV_BND_MAIN

    data = [
        {
            "nrow": "001",
            "vcfnum": "058",
            "Tier": "1",
            "Start": "2:211,842,747",
            "End": "2:214,067,957",
            "BND_ID": "092",
            "BND_mate": "B",
            "Genes": "ERBB4, SPAG16",
            "Effect": "FrameshiftV, FusG",
            "Detail": "known_promiscuous",
            "SR_alt": 2,
            "PR_alt": 2,
            "AF_BPI": "0.02 (0.02, 0.02)",
            "SR_PR_ref": "114,86",
            "SScore": 30,
            "ntrx": 1,
            "Transcript": "ENST00000342788_exon_3/27"
        },
        {
            "nrow": "002",
            "vcfnum": "062",
            "Tier": "1",
            "Start": "2:214,067,957",
            "End": "2:211,842,747",
            "BND_ID": "092",
            "BND_mate": "A",
            "Genes": "ERBB4, SPAG16",
            "Effect": "FrameshiftV, FusG",
            "Detail": "known_promiscuous",
            "SR_alt": 2,
            "PR_alt": 2,
            "AF_BPI": "0.02 (0.02, 0.02)",
            "SR_PR_ref": "114,86",
            "SScore": 30,
            "ntrx": 1,
            "Transcript": "ENST00000331683_exon_13/15"
        },
    ]


class SvBNDPurpleinfReportFactory(ReportFactory):
    type = ReportType.SV_BND_PURPLEINF

    data = [
    ]


class SvNoBNDMainReportFactory(ReportFactory):
    type = ReportType.SV_NOBND_MAIN

    data = [
        {
            "nrow": "0001",
            "vcfnum": "342",
            "TierTop": "2",
            "Tier": "2",
            "Type": "DEL",
            "Start": "16:16,955,318",
            "End": "16:73,728,847",
            "Effect": "ChromNumV",
            "Genes": "16",
            "Transcript": "",
            "Detail": "chrom_16",
            "SR_alt": 2,
            "PR_alt": 2
        },
        {
            "nrow": "0002",
            "vcfnum": "344",
            "TierTop": "2",
            "Tier": "2",
            "Type": "DUP",
            "Start": "16:21,850,260",
            "End": "16:29,498,554",
            "Effect": "Dup",
            "Genes": "16",
            "Transcript": "",
            "Detail": "chrom_16",
            "PR_alt": 15
        },
    ]


class SvNoBNDOtherReportFactory(ReportFactory):
    type = ReportType.SV_NOBND_OTHER

    data = [
        {
            "vcfnum": "342",
            "AF_BPI": "0.03 (0.03, 0.03)",
            "SR_PR_ref": "0,87",
            "SScore": 31
        },
        {
            "vcfnum": "344",
            "AF_BPI": "0.16 (0.21, 0.12)",
            "SR_PR_ref": "NA,91",
            "SScore": 55
        },
        {
            "vcfnum": "373",
            "AF_BPI": "0.03 (0.04, 0.02)",
            "SR_PR_ref": "152,98",
            "SScore": 38
        },
        {
            "vcfnum": "053",
            "AF_BPI": "0.03 (0.01, 0.04)",
            "SR_PR_ref": "201,150",
            "SScore": 32
        },
    ]


class SvNoBNDManyGenesReportFactory(ReportFactory):
    type = ReportType.SV_NOBND_MANYGENES

    data = [
        {
            "nrow": "0017",
            "vcfnum": "053",
            "Tier": "2",
            "Type": "DEL",
            "Start": "2:170,871,570",
            "End": "2:178,602,277",
            "Effect": "DelG",
            "ngen": 162,
            "Genes": "AC106900.6, RP11-387A1.5, WIPF1, FUCA1P1, HOXD12, RNU5E-9P, AC011998.2, AC017048.2, HNRNPA3, RPSAP24, CDCA7, RPL21P38, RP11-451F14.1, METTL8, HOXD11, DNAJC19P5, AC009948.7, RP11-394I13.3, AC008065.1, AC007969.5, AC012499.1, AC096649.1, RP11-796E10.1, AC016751.3, AC104088.1, DLX1, RP11-171I2.2, snoU13, HOXD1, RAPGEF4, TTN, SP9, H3F3AP4, AC009948.5, RP11-1O7.1, AC013461.1, AC073465.3, AGPS, HAT1, RNU6-5P, PDK1, Y_RNA, RNU6-182P, AC078883.4, PRKRA, AC074286.1, DCAF17, GORASP2, AC010894.5, RP11-65L3.3, AC010894.4, DFNB59, RAPGEF4-AS1, RP11-337N6.1, GPR155, AC092573.2, AC079305.10, SCRN3, HOXD13, AC096649.3, ATP5G3, AC073834.3, RNU6-187P, AC009336.19, uc_338, RNU6ATAC14P, HAGLROS, MIR10B, RNU6-763P, AC017048.1, FKBP7, AC013410.2, AC013410.1, RP11-483E17.1, AC073636.1, PDE11A, SLC25A12, AC079305.11, RP11-387A1.6, DLX2, RP11-337N6.2, LINC01117, RBM45, HAGLR, OSBPL6, RPSAP25, AC013467.1, RN7SL65P, HOXD3, OLA1, PLEKHA3, MIR933, RP11-394I13.2, RP11-632P5.1, HOXD-AS2, SP3, RNU6-629P, TTC30A, TTN-AS1, AC019080.1, AC096649.2, RP11-171I2.5, AC092162.2, RP11-227L6.1, TTC30B, METAP1D, RNU6-1290P, HOXD4, AC011998.1, AC011998.4, RP11-572N21.1, RP11-6C10.1, HOXD10, AC093818.1, AC078883.3, API5P2, TLK1, LINC01305, MIR1246, RP11-394I13.1, RP11-324L17.1, LNPK, AC092162.1, AC010894.3, AC079305.8, NFE2L2, MLK7-AS1, AC073465.2, DLX2-AS1, DYNC1I2, CHRNA1, KRT8P40, AC007969.4, ITGA6, CIR1, AC079305.5, EVX2, ATF2, AC016751.2, RP11-65L3.2, ALDH7A1P2, EXTL2P1, HOXD8, CHN1, MIR3128, AC018712.2, RP11-65L3.4, AC073069.2, HNRNPA1P39, MIR6512, DAP3P2, AC073465.1, CYCTP, LINC01116, MTX2, HOXD9, RPL5P7, RNA5SP112, AC016739.2, AC018890.6, RP11-337N6.3, CYBRD1"
        },
        {
            "nrow": "0018",
            "vcfnum": "400",
            "Tier": "2",
            "Type": "DEL",
            "Start": "22:22,653,819",
            "End": "22:24,634,811",
            "Effect": "DelG",
            "ngen": 138,
            "Genes": "AP000349.2, IGLJ5, IGLV3-22, IGLV3-9, AP000345.4, IGLV3-24, MIF-AS1, ADORA2A, IGLJ2, LL22NC03-84E4.8, RN7SL268P, AP000345.1, PCAT14, IGLC4, SUSD2, MIR5571, IGLV2-18, IGLV2-8, RAB36, ZDHHC8P1, KB-1572G7.2, BCRP1, IGLC1, IGLV3-21, IGLV3-27, LRRC75B, ASLP1, POM121L9P, FBXW4P1, GSTT2B, Metazoa_SRP, CES5AP1, KB-1572G7.5, AP000355.2, GGT5, IGLJ7, GSTTP1, ADORA2A-AS1, IGLV3-12, AP000345.2, GNAZ, AP000350.10, KB-1572G7.4, GGT1, U7, GUSBP11, LL22NC03-48A11.14, MMP11, IGLVI-20, AP000343.2, IGLC2, SNRPD3, BCRP8, AP000344.3, IGLV3-6, AP000343.1, AP000356.2, IGLV3-15, BCRP3, IGLV3-13, IGLVVI-22-1, AP000362.1, KB-1269D1.8, SPECC1L-ADORA2A, BCR, AP000344.4, GGTLC4P, IGLC7, IGLV3-1, LL22NC03-24A12.8, RGL4, IGLC6, AP000351.4, AP000351.13, LL22NC03-84E4.13, AP000351.3, C22orf15, RN7SL263P, SMARCB1, AP000350.6, KB-226F1.2, IGLV3-7, MIF, LL22NC03-102D1.16, DDT, RSPH14, IGLV3-2, IGLV2-14, GSTT2, DRICH1, IGLV3-10, LL22NC03-84E4.11, AP000347.2, IGLJ1, SLC2A11, IGLV2-11, IGLV3-4, KB-1125A3.11, IGLJ3, IGLC3, AP000350.7, SPECC1L, AP000350.8, KB-1572G7.3, DERL3, AP000350.5, IGLC5, CHCHD10, IGLL1, IGLV3-25, KB-318B8.7, VPREB3, GUCD1, IGLL5, KB-208E9.1, CABIN1, ZNF70, IGLVVI-25-1, IGLV3-19, KB-1125A3.12, LL22NC03-24A12.9, IGLJ6, IGLV3-26, UPB1, IGLJ4, KB-1125A3.10, IGLV2-23, AP000347.4, DDTL, IGLV2-5, IGLV3-29, IGLV4-3, AP000361.2, IGLV3-16, LL22NC03-102D1.18, IGLV3-17, IGLV2-28, AP000354.2"
        },
    ]


class SvNoBNDManyTranscriptsReportFactory(ReportFactory):
    type = ReportType.SV_NOBND_MANYTRANSCRIPTS

    data = [
        {
            "nrow": "0035",
            "vcfnum": "342",
            "Tier": "2",
            "Type": "DEL",
            "Start": "16:16,955,318",
            "End": "16:73,728,847",
            "Effect": "DelTx",
            "ntrx": 3,
            "Transcript": "ENST00000427738_exon_6/18, ENST00000398568_exon_6/18, ENST00000311559_exon_8/20"
        },
        {
            "nrow": "0046",
            "vcfnum": "342",
            "Tier": "2",
            "Type": "DEL",
            "Start": "16:16,955,318",
            "End": "16:73,728,847",
            "Effect": "DelTx",
            "ntrx": 3,
            "Transcript": "ENST00000254108_exon_5/15, ENST00000568685_exon_5/15, ENST00000380244_exon_4/15"
        },
    ]
