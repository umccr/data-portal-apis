# Misc Portal Lambda

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
