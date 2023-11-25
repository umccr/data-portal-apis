# Oncoanalyser WGS

See automated pipeline [trigger and input dependency diagram](../README.md) for upstream/downstream workflow dependencies and what might it get trigger automatically.

## Option 1

We can re-enter the Pipeline from _some_ Oncoanalyser WGS step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/oncoanalyser_wgs.py#L78-L88)
  - _Attached [oncoanalyser_wgs_payload.json](oncoanalyser_wgs_payload.json) here for convenience_


_Step 2)_
- Find DRAGEN `wgs_tumor_normal` output BAMs of given Subject from `/gds` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/gds/?rowsPerPage=1000&subject=SBJ04417&search=tumor%20normal%20.bam%24" | jq
```

- Alternatively, you can search through Portal Subject Data view e.g.
  - Go to https://portal.umccr.org/subjects/SBJ04417/subject-data
  - At "GDS" tab > Click "t/n bam" button > Click 3 bar action > Copy URI

- Alternatively, you can infer from DRAGEN `wgs_tumor_normal` workflow output e.g.
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/workflows?type_name=wgs_tumor_normal&portal_run_id=20231122f6cc369b" | jq
```


_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-oncoanalyser_wgs \
  --cli-binary-format raw-in-base64-out \
  --payload file://oncoanalyser_wgs_payload.json \
  out.json
```

## Option 2

If you can not find appropriate DRAGEN `wgs_tumor_normal` output BAM, then you can trigger one. See [tumor_normal.md](tumor_normal.md) 
