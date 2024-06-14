# DRAGEN Alignment QC

_(See [automated_pipeline.svg](../../model/automated_pipeline.svg))_


## Notes

- See [metadata.md](../metadata.md) note for NTC/PTC sample treatment.
- With Portal Ocicat release, the workflow does not differentiate between WGS or WTS. Hence, "DRAGEN Alignment QC".
- We run DRAGEN Alignment QC at per "lane" level per sequencing run for WGS library.
- WTS tumor library are typically single lane.  


## Option 1

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/dragen_wgs_qc.py#L75-L96)
  - _Attached [dragen_alignment_qc.json](dragen_alignment_qc.json) here for convenience_
- The `batch_run_id` attribute is optional.

_Step 2)_
- Find corresponding `SequenceRun` info from `/sequencerun` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/sequencerun?instrument_run_id=240607_A01052_0209_BHLHFTDSXC&status=PendingAnalysis" | jq
```

_Step 3)_
- Find library primary data from `/fastq` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/fastq?rglb=L2100878&lane=2&sequence_run__instrument_run_id=240607_A01052_0209_BHLHFTDSXC" | jq
```

_Step 4)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke \
  --function-name data-portal-api-prod-dragen_wgs_qc \
  --cli-binary-format raw-in-base64-out \
  --payload file://dragen_alignment_qc.json \
  out.json
```


## Option 2

_Meanwhile, please ask for orchestrator batch trigger option availability in Slack `#data-portal`._
