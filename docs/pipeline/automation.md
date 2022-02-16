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

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-orchestrator \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "wfr_id": "wfr.e7cd80eee78e425ca94507f505315e9b", 
    "wfv_id": "wfv.f776566ff2b94edeab92b25b33b89eb8", 
    "skip": ["GOOGLE_LIMS_UPDATE_STEP"]
    }' \
  orchestrator_197.json
```

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

### Step Skip

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
