# Transcriptome

_a.k.a. DRAGEN transcriptome workflow or WTS_

### Option 1

We can re-enter the Pipeline from _some_ Transcriptome step as follows.

_Step 1)_
- To prepare event payload JSON as required in [Lambda payload schema](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/dragen_wts.py#L61-L81)
  - _Attached [transcriptome_payload.json](transcriptome_payload.json) here for convenience_

_Step 2)_
- You should know related SequenceRun info for the target WTS tumor library ID. See `/sequencerun` or `/sequence` endpoints.
- You will need to work out the required _Fastq List Rows_ model.
- Generally, _Fastq List Rows_ details can be inferred from primary step output i.e. `/fastq` endpoint
- For this
  - You may wish to check [examples/fastq_list_row.R](../../examples/fastq_list_row.R) as starter
  - And in conjunction Portal `/metadata` endpoint
- Alternatively, you could use **Portal Athena Query** with SQL, if you prefer. 
  - See "Saved queries" tab for starter. Or see `*.sql` scripts in [example folder](../../examples).
- You may ignore `batch_run_id` as we are not running it as a Batch manner. Just this WTS library only.
- If any other doubts, please feel free to reach out in `#bioinfo` channel

_Step 3)_
- Then, to hit the Lambda as follows.

```
aws lambda invoke --profile prod \
  --function-name data-portal-api-prod-dragen_wts \
  --cli-binary-format raw-in-base64-out \
  --payload file://transcriptome_payload.json \
  out.txt
```

### Option 2

We can also to re-enter (restart) from Orchestrator such that knowing of previous step.

- In this case, BCL Convert workflow run ID as payload to it.
  - https://github.com/umccr/data-portal-apis/tree/dev/docs/pipeline/automation#orchestrator
- Then, Pipeline will continue from then on.
