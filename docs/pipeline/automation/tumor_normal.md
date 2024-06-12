# Tumor Normal ICA Pipeline Lambda

_(See [automated_pipeline.svg](../../model/automated_pipeline.svg))_


## Notes

- See [metadata.md](../metadata.md) note for NTC/PTC sample treatment.
- In most recent cases, you should be able to drive Tumor/Normal trigger through Portal Launch Pad UI.
- The following options are alternates; when you need to "pair" sample manually and, like to trigger them - [ditto](https://umccr.slack.com/archives/CP356DDCH/p1698793560313359).
- Pairing algorithm
  - Topup (library having `_topup`) are merged with previous libraries of the same name
  - No logic yet for reruns (i.e. it will skip library having `_rerun` suffix)
  - https://github.com/umccr/data-portal-apis/pull/262
  - https://github.com/umccr/data-portal-apis/pull/596


## Option 1

We can re-enter the Pipeline from _some_ Tumor Normal step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/tumor_normal.py#L77-L114)
  - _Attached [tumor_normal_payload.json](tumor_normal_payload.json) here for convenience_

_Step 2)_
- Get FASTQs details from `/fastq` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/fastq?rglb=L2301046&rglb=L2301047&rglb=L2301362" | jq
```

- Alternatively, use **Portal Athena Query** with SQL, if you prefer.
```sql
select * from "data_portal_fastqlistrow" where rglb in ('L2301046', 'L2301047', 'L2301362');
```

_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-tumor_normal \
  --cli-binary-format raw-in-base64-out \
  --payload file://tumor_normal_payload.json \
  out.json
```

### Pairing Endpoint

From Option 1 of Step 2 above;

- Alternatively, you can get automated FASTQ pairs from `/pairing` endpoint.
- Use of this option is preferable when/if possible. 
- However, any need of "manual pairing" override and case like multiple germline; then this automated pairing option won't do.
- This automated pairing has coded so that it fits for most clinical and research pairing cases:
  - 1 germline, multiple somatic
  - topup will be merged
  - clinical germline, research somatic


- By Subject:
```bash
curl -s -X POST -d '["SBJ04311"]' \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/pairing/by_subjects" | jq
```

- By Library:
```bash
curl -s -X POST -d '["L2301046", "L2301047", "L2301362"]' \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/pairing/by_libraries" | jq
```


## Option 2

_Expert Mode_

We can also to re-enter (restart) from Orchestrator such that knowing of previous step.
- In this case, WGS QC (`wgs_alignment_qc`) workflow run ID as payload to it.
- Then, Pipeline will continue from then on.
- See [README.md#orchestrator](README.md#orchestrator)


## Example Scripts

- [examples/fastq_list_row.R](../../examples/fastq_list_row.R)
- [examples/pairing_tn_fastq.R](../../examples/pairing_tn_fastq.R)
- https://github.com/umccr/biodaily/tree/main/WGS_accreditation/seqc_dilution_run_samples_2
- https://github.com/umccr/biodaily/pull/58
- https://github.com/umccr/biodaily/pull/69
- https://github.com/umccr/biodaily/tree/main/pdiakumis/tumor_normal_submission
