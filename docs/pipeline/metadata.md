# Portal Metadata

Portal has data processing Lambdas for scheduled or on-demand sync with upstream metadata sources.

### LIMS

LIMS update lambda is idempotent i.e. you can hit as many times as you like. Portal will sync latest (merge & update) from Google LIMS sheet1.

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-lims_scheduled_update_processor \
  --invocation-type Event \
  lims_update.json
```

### Lab Metadata

Lab metadata update lambda is idempotent i.e. you can hit as many times as you like. Portal will sync latest (truncate & reload) from Google Lab tracking sheets - from 2019 upto now.

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-labmetadata_scheduled_update_processor \
  --invocation-type Event \
  meta_update.json
```

Alternatively, you can use **SampleSheet Checker UI** as follows.

- Goto https://sscheck.umccr.org
- Login using umccr.org (i.e. usual Portal Login)
- Click button next to label "Sync Portal Metadata"
