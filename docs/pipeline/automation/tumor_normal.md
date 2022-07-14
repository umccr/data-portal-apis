# Tumor Normal ICA Pipeline Lambda

### Option 1

We can re-enter the Pipeline from _some_ Tumor Normal step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/tumor_normal.py#L64-L104)
  - _Attached [tumor_normal_payload.json](tumor_normal_payload.json) here for convenience_

_Step 2)_
- User will need to figure out Tumor and Normal pairing in regard to _Fastq List Rows_ model
  - Generally, _Fastq List Rows_ for Tumor and Normal details can be inferred from primary step output i.e. `/fastq` endpoint
    - For this, and **manually pairing the samples**
      - You may wish to check [examples/fastq_list_row.R](../../examples/fastq_list_row.R) as starter
      - And in conjunction Portal `/metadata` endpoint
  - Pipeline also expose **automated pairing** through `/pairing` endpoint
    - For this, you may wish to check [examples/pairing_tn_fastq.R](../../examples/pairing_tn_fastq.R) as starter
- Alternatively, you could use **Portal Athena Query** with SQL, if you prefer. 
  - See "Saved queries" tab for starter. Or see `*.sql` scripts in [example folder](../../examples).

_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-tumor_normal \
  --cli-binary-format raw-in-base64-out \
  --payload file://tumor_normal_payload.json \
  out.txt
```

### Option 2

We can also to re-enter (restart) from Orchestrator such that knowing of previous step.

- In this case, one of WGS alignment QC workflow run ID in a batch as payload to it.
  - https://github.com/umccr/data-portal-apis/tree/dev/docs/pipeline/automation#orchestrator
- Then, Pipeline will continue from then on.

### Option 3

Finally, one can launch WES workflow run by preparing and specifying payload through more general [wes_handler](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/wes_handler.py#L26-L39) Lambda.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-wes_launch \
  --cli-binary-format raw-in-base64-out \
  --payload file://my_workflow_payload.json \
  out.txt
```

However, at this point, it is recommended to fallback using ICA CLI or, ica-lazy, etc.. which might be better user interfacing there in CLI.
