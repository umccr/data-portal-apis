# Transcriptome

_a.k.a. DRAGEN transcriptome workflow or `wts_tumor_only` workflow_


_(See [automated_pipeline.svg](../../model/automated_pipeline.svg))_


## Notes

- See [metadata.md](../metadata.md) note for NTC/PTC sample treatment.
- Since Portal 2.2 (Ocicat) release, DRAGEN transcriptome trigger business logic has updated as follows.
  - For **topup** library, it will trigger "merged" with the original (initially) sequenced WTS library.
  - For **rerun** library, it will skip trigger all together. Hence, it requires manual trigger follow up; as well as for the corresponding RNAsum.
  - DRAGEN transcriptome workflow is triggered in Subject-wide manner. 
    - i.e. It could go across different sequencing runs to find matching WTS library (e.g. topup) of a given subject.


## Option 1

We can re-enter the Pipeline from _some_ Transcriptome step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/dragen_wts.py#L78-L97)
  - _Attached [transcriptome_payload.json](transcriptome_payload.json) here for convenience_

_Step 2)_
- Get FASTQs details from `/fastq` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?rglb=L2301353" | jq
```

- Alternatively, use **Portal Athena Query** with SQL, if you prefer.
```sql
select * from "data_portal_fastqlistrow" where rglb = 'L2301353';
```

_Step 3)_
- Set `arriba_large_mem` flag to `true` if you need to run arriba using the `standardHiMem:medium` 

_Step 4)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-dragen_wts \
  --cli-binary-format raw-in-base64-out \
  --payload file://transcriptome_payload.json \
  out.json
```


## Option 2

_Expert Mode_

We can also to re-enter (restart) from Orchestrator such that knowing of previous step.
- In this case, WTS QC (`wts_alignment_qc`) workflow run ID as payload to it.
- Then, Pipeline will continue from then on.
- See [README.md#orchestrator](README.md#orchestrator)


## Example Scripts

- [examples/fastq_list_row.R](../../examples/fastq_list_row.R)
