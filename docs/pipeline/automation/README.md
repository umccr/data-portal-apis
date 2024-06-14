# Portal Automation Lambda

- Multiple entrypoints into the Pipeline (workflow automation) is possible. This is exposed as invoking a Lambda.
- Depends on the step entry, it may impose a bit of complexity setting up the required Lambda event payload.
- Pipeline will pick up the workflow event, as long as a workflow run name start with `umccr__automated` into the Pipeline orchestration.
- These different entry points are designed in mind such that — when we need to run workflow manually as _out-of-band_ cases but, still align as in the main Pipeline semantic or, get recorded into the Pipeline database.

## Primary Stage

### ICA Event Controller

```
aws lambda invoke --profile dev \
  --function-name data-portal-api-dev-sqs_iap_event_processor \
  --cli-binary-format raw-in-base64-out \
  --payload file://bssh_sqs_event_replay.json \
  out.txt
```

### LibraryRun

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-libraryrun \
  --cli-binary-format raw-in-base64-out \
  --payload '{
     "gds_volume_name": "umccr-raw-sequence-data-dev", 
     "gds_folder_path": "/200612_A01052_0017_BH5LYWDSXY_r.Uvlx2DEIME-KH0BRyF9XBg",
     "instrument_run_id": "200612_A01052_0017_BH5LYWDSXY", 
     "run_id": "r.Uvlx2DEIME-KH0BRyF9XBg"
     }' \
  libraryrun_lambda_output.json
```

### BCL Convert

```
aws lambda invoke --profile dev \
  --function-name data-portal-api-dev-bcl_convert \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "gds_volume_name": "umccr-raw-sequence-data-dev",
    "gds_folder_path": "/200612_A01052_0017_BH5LYWDSXY",
    "seq_run_id": "r.AnPk0ox1OU-pACGtvSEQC2",
    "seq_name": "200612_A01052_0017_BH5LYWDSXY"
  }' \
  bcl_convert_lambda_output.json
```

### Orchestrator

NOTES:

1. Here `wfr.<ID>` is the Workflow Run ID of one step before your next target step that you wish to trigger.
2. And `wfv.<ID>` correspond to Workflow Version ID counterpart from point 1 Workflow Run ID.
3. You can skip steps. See Pipeline [Concept](../README.md) and **Pipeline Control** mechanism (see below section).

You should get this information typically through correspond Slack notification from ICA Pipeline Automated Workflow Event in `#biobot`. 

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-orchestrator \
  --cli-binary-format raw-in-base64-out \
  --payload '{
      "wfr_id": "wfr.<ID>", 
      "wfv_id": "wfv.<ID>", 
      "skip": { "global": ["UPDATE_STEP", "FASTQ_UPDATE_STEP", "GOOGLE_LIMS_UPDATE_STEP", "DRAGEN_WGS_QC_STEP", "DRAGEN_TSO_CTDNA_STEP"] }
    }' \
  orchestrator_197.json
```

## Secondary and Tertiary Analysis Stage

> NOTE: Others Lambda entry points are possible. We are documenting it as we speak. Please look into [their docstring](../../../data_processors/pipeline/lambdas) for event payload requirement. Typically, we drive (i.e. restart/resume/rerun) the step from Orchestrator for post BCL conversion steps.

### ctTSO

See [tso_ctdna.md](tso_ctdna.md)

### DRAGEN Alignment QC

See [dragen_alignment_qc.md](dragen_alignment_qc.md)

### Tumor Normal

See [tumor_normal.md](tumor_normal.md)

### Umccrise

See [umccrise.md](umccrise.md)

### Transcriptome

_aka WTS_

See [transcriptome.md](transcriptome.md)

### RNAsum

See [rnasum.md](rnasum.md)

### Star Alignment

See [star_alignment.md](star_alignment.md)

### Oncoanalyser WTS

See [oncoanalyser_wts.md](oncoanalyser_wts.md)

### Oncoanalyser WGS

See [oncoanalyser_wgs.md](oncoanalyser_wgs.md)


## Post Analysis Stage

### Somalier

_Call Somalier 'extract' through Holmes orchestration pipeline_

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-somalier_extract \
  --cli-binary-format raw-in-base64-out \
  --payload '{"reference": "hg38.rna", "index": "gds://production/analysis_data/SBJ02296/wgs_tumor_normal/20220605e40c7f62/L2200674_L2200673_dragen/PRJ221207_tumor.bam"}' \
  out_extract.json
```

## Pipeline Control

### Emergency Stop

- This will stop ongoing sequencing `200612_A01052_0017_BH5LYWDSXY` Run to stop further processing analysis workflows, including BCL conversion.
- Payload with `[]` to reset.

```
aws ssm put-parameter \
  --name "/iap/workflow/emergency_stop_list" \
  --type "String" \
  --value "[\"200612_A01052_0017_BH5LYWDSXY\"]" \
  --overwrite \
  --profile dev
```

### Step Skip

- Pipeline can skip some steps globally or by Sequence Run context. For example:

```
aws ssm put-parameter \
  --name "/iap/workflow/step_skip_list" \
  --type "String" \
  --value "{ \"global\": [ \"DRAGEN_WGS_QC_STEP\", \"DRAGEN_TSO_CTDNA_STEP\"] }" \
  --overwrite \
  --profile dev
```

- Some possible example config in JSON as follows.

```json
{
  "global": [
    "UPDATE_STEP",
    "FASTQ_UPDATE_STEP",
    "GOOGLE_LIMS_UPDATE_STEP",
    "DRAGEN_WGS_QC_STEP",
    "DRAGEN_TSO_CTDNA_STEP",
    "DRAGEN_WTS_STEP",
    "TUMOR_NORMAL_STEP",
    "UMCCRISE_STEP",
    "RNASUM_STEP",
    "SOMALIER_EXTRACT_STEP"
  ],
  "by_run": {
    "220524_A01010_0998_ABCF2HDSYX": [
      "FASTQ_UPDATE_STEP",
      "GOOGLE_LIMS_UPDATE_STEP",
      "DRAGEN_WGS_QC_STEP",
      "DRAGEN_TSO_CTDNA_STEP",
      "DRAGEN_WTS_STEP"
    ],
    "220525_A01010_0999_ABCF2HDSYX": [
      "UPDATE_STEP",
      "FASTQ_UPDATE_STEP",
      "GOOGLE_LIMS_UPDATE_STEP",
      "DRAGEN_WGS_QC_STEP",
      "DRAGEN_TSO_CTDNA_STEP",
      "DRAGEN_WTS_STEP"
    ]
  }
}
```

- Payload with `{}` to reset.

```
aws ssm put-parameter \
  --name "/iap/workflow/step_skip_list" \
  --type "String" \
  --value "{}" \
  --overwrite \
  --profile dev
```
