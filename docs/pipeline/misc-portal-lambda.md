# Misc Portal Lambda

### Fastq lambda

FASTQ lambda work like `ls -l` to list the output for BCL Convert FASTQs as follows.

```
aws lambda invoke --profile dev \
  --function-name data-portal-api-dev-fastq \
  --cli-binary-format raw-in-base64-out \
  --payload '{"locations": ["gds://umccr-fastq-data-dev/200227_A00130_0131_BHYTCVDSXX"]}' \
  fastq_output_0131.json
```

See [fastq_lambda.R](../examples/fastq_lambda.R) for more.

### LIMS update

LIMS update lambda is idempotent i.e. you can hit as many times as you like. Portal will sync latest from Google LIMS.

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-lims_scheduled_update_processor \
  --invocation-type Event \
  lims_update.json
```

### Lab metadata update

Lab metadata update lambda is idempotent i.e. you can hit as many times as you like. Portal will sync latest from Google Lab tracking sheet.

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-labmetadata_scheduled_update_processor \
  --invocation-type Event \
  meta_update.json
```
