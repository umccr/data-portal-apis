# TSO ctDNA

_ADVANCED_

> NOTE: To trigger DRAGEN `tso_ctdna_tumor_only` workflow requires active SequenceRun raw data path having the following meta info files.
> - RunInfo.xml
> - RunParameters.xml
> 
> Since we may have already archived raw data, these file locations won't be available anymore. It requires to restore these meta files from archival location and prepare them in active GDS location.


## Option 1

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/dragen_tso_ctdna.py#L75-L114)
  - _Attached [tso_ctdna_payload.json](tso_ctdna_payload.json) here for convenience_
- The attributes `seq_run_id`, `seq_name`, `batch_run_id` are optional.


_Step 2)_
- Find Subject primary data from `/fastq` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/fastq?rglb=L2101400" | jq
```

- Find corresponding split SampleSheet BCL_Convert workflow output from `/workflows` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/workflows?portal_run_id=202112164c162d5f" | jq
```

- Find raw data path from `/sequencerun` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/sequencerun?instrument_run_id=211118_A01052_0064_BH372GDMXY&status=PendingAnalysis" | jq
```

If raw data path is no longer available as recorded in SequenceRun response then you can restore meta files from archival to active GDS location. 


_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-dragen_tso_ctdna \
  --cli-binary-format raw-in-base64-out \
  --payload file://tso_ctdna_payload.json \
  out.json
```


## Option 2

Restart the whole batch through orchestrator Lambda to simulate after bcl_convert succeeded condition. Warning!! This is expensive ops, unless we really need most of the samples within the batch.
