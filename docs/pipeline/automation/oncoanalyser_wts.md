# Oncoanalyser WTS

See automated pipeline [trigger and input dependency diagram](../README.md) for upstream/downstream workflow dependencies and what might it get trigger automatically.

## Option 1

We can re-enter the Pipeline from _some_ Oncoanalyser WTS step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/oncoanalyser_wts.py#L78-L85)
  - _Attached [oncoanalyser_wts_payload.json](oncoanalyser_wts_payload.json) here for convenience_


_Step 2)_
- Find Star Alignment output tumor BAM of given Subject from `/s3` endpoint
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/s3/?rowsPerPage=1000&subject=SBJ04417&search=star%20.bam%24" | jq
```

- Alternatively, you can search through Portal Subject Data view e.g.
  - Go to https://portal.umccr.org/subjects/SBJ04417/subject-data
  - Click "S3" tab > at "Search Filter" box > enter `star .bam$` 

- Alternatively, you can infer from Star Alignment workflow output e.g.
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" \
  "https://api.portal.prod.umccr.org/workflows?type_name=star_alignment&library_id=L2301387" | jq
```


_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-oncoanalyser_wts \
  --cli-binary-format raw-in-base64-out \
  --payload file://oncoanalyser_wts_payload.json \
  out.json
```

## Option 2

If you can not find appropriate Star Alignment output BAM, then you can trigger one. See [star_alignment.md](star_alignment.md) 
