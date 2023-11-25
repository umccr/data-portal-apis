# Umccrise

_a.k.a. umccr cancer report workflow_

### Option 1

We can re-enter the Pipeline from _some_ umccrise step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/umccrise.py#L75-L92)
  - _Attached [umccrise_payload.json](umccrise_payload.json) here for convenience_

_Step 2)_
- You should know related Tumor Normal workflow run "output" and, you can query `/workflow` endpoint.
```bash
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/workflows?portal_run_id=202311177fe4c1bc" | jq
```

- Alternatively, you could use **Portal Athena Query** with SQL, if you prefer.
```sql
select * from "data_portal_workflow" where portal_run_id = '202311177fe4c1bc';
```

- Then using Tumor Normal workflow output directory, you can prepare `dragen_somatic_directory` and `dragen_germline_directory` locations.

_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-umccrise \
  --cli-binary-format raw-in-base64-out \
  --payload file://umccrise_payload.json \
  out.json
```


### Option 2

_Expert Mode_

We can also to re-enter (restart) from Orchestrator such that knowing of previous step.
- In this case, WGS Tumor Normal (`wgs_tumor_normal`) workflow run ID as payload to it.
- Then, Pipeline will continue from then on.
- See [README.md#orchestrator](README.md#orchestrator)
