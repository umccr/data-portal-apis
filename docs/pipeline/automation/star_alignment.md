# Star Alignment

See automated pipeline [trigger and input dependency diagram](../README.md) for upstream/downstream workflow dependencies and what might it get trigger automatically.

## Option 1

We can re-enter the Pipeline from _some_ Star Alignment step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/star_alignment.py#L78-L87)
  - _Attached [star_alignment_payload.json](star_alignment_payload.json) here for convenience_

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
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-star_alignment \
  --cli-binary-format raw-in-base64-out \
  --payload file://star_alignment_payload.json \
  out.json
```
