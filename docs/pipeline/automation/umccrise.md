# Umccrise

_a.k.a. umccr cancer report workflow_

### Option 1

We can re-enter the Pipeline from _some_ umccrise step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/umccrise.py#L63-L90)
  - _Attached [umccrise_payload.json](umccrise_payload.json) here for convenience_

_Step 2)_
- You should know related Tumor Normal (Somatic) workflow run output for the target Subject ID. You can query `/workflow` endpoint for that.
- You will need to work out the required _Fastq List Rows_ model.
- Generally, _Fastq List Rows_ details can be inferred from primary step output i.e. `/fastq` endpoint
- For this
  - You may wish to check [examples/fastq_list_row.R](../../examples/fastq_list_row.R) as starter
  - And in conjunction Portal `/metadata` endpoint
- Alternatively, you could use **Portal Athena Query** with SQL, if you prefer. 
  - See "Saved queries" tab for starter. Or see `*.sql` scripts in [example folder](../../examples).

_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-umccrise \
  --cli-binary-format raw-in-base64-out \
  --payload file://umccrise_payload.json \
  out.txt
```

### Option 2

We can also to re-enter (restart) from Orchestrator such that knowing of previous step.

- In this case, one of Tumor Normal workflow run ID as payload to it.
  - https://github.com/umccr/data-portal-apis/tree/dev/docs/pipeline/automation#orchestrator
- Then, Pipeline will continue from then on.
