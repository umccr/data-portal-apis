# Portal Automation Lambda

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
3. You can skip steps. See Pipeline [Concept](../README.md).

You should get this information typically through correspond Slack notification from ICA Pipeline Automated Workflow Event in `#biobot`. 

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-orchestrator \
  --cli-binary-format raw-in-base64-out \
  --payload '{
      "wfr_id": "wfr.<ID>", 
      "wfv_id": "wfv.<ID>", 
      "skip": ["GOOGLE_LIMS_UPDATE_STEP", "DRAGEN_WGS_QC_STEP"]
    }' \
  orchestrator_197.json
```

### RNAsum

See [rnasum.md](rnasum.md)

### Other Lambda

Others Lambda are possible. Look into their docstring for event payload requirement. Typically, we drive (i.e. restart/resume/rerun) the step from Orchestrator for post BCL conversion steps.

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

### Global Step Skip

- This will globally skip `TUMOR_NORMAL_STEP` and `DRAGEN_WTS_STEP` analysis workflows.
- Payload with `[]` to reset.

```
aws ssm put-parameter \
  --name "/iap/workflow/step_skip_list" \
  --type "String" \
  --value "[\"TUMOR_NORMAL_STEP\", \"DRAGEN_WTS_STEP\"]" \
  --overwrite \
  --profile dev
```
