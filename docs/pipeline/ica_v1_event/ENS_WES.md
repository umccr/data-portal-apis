# WES Events Subscription

- At the moment, Portal only need WES Run event type denote `wes.runs`
- We subscribe to `created,aborted,updated` actions

## DEV

- DEV subscription use `development` project context
- Created as follows:
```
ica projects enter development

ica subscriptions create \
  --name "UMCCRWESRunsEventDataPortalDevProject" \
  --type "wes.runs" \
  --actions "created,aborted,updated" \
  --description "UMCCR data portal (DEV) subscribed to wes.runs using the development project" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<DEV_ACCOUNT_ID>/data-portal-dev-iap-ens-event-queue" \
  --filter-expression "{\"startsWith\":[{\"path\":\"$.WorkflowRun.Name\"},\"umccr__automated\"]}"
```

- Get:
```
ica subscriptions get sub.1694 -ojson | jq
```

## PROD

- PROD subscription use `production` project context
- Created as follows:
```
ica projects enter production

ica subscriptions create \
  --name "UMCCRWESRunsEventDataPortalProdProject" \
  --type "wes.runs" \
  --actions "created,aborted,updated" \
  --description "UMCCR data portal (PROD) subscribed to wes.runs events using the production project" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<PROD_ACCOUNT_ID>/data-portal-prod-iap-ens-event-queue" \
  --filter-expression "{\"startsWith\":[{\"path\":\"$.WorkflowRun.Name\"},\"umccr__automated\"]}"
```

- Get:
```
ica subscriptions get sub.1696 -ojson | jq
```

## STG

- STG subscription use `staging` project context
- Created as follows:
```
ica projects enter staging

ica subscriptions create \
  --name "UMCCRWESRunsEventDataPortalStgProject" \
  --type "wes.runs" \
  --actions "created,aborted,updated" \
  --description "UMCCR data portal (STG) subscribed to wes.runs events using the staging project" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<STG_ACCOUNT_ID>/data-portal-stg-iap-ens-event-queue" \
  --filter-expression "{\"startsWith\":[{\"path\":\"$.WorkflowRun.Name\"},\"umccr__automated\"]}"
```

- Get:
```
ica subscriptions get sub.2479 -ojson | jq
```
