# GDS Events Subscription

- At the moment, Portal only need GDS File event type denote `gds.files`
- We subscribe to `uploaded,deleted,archived,unarchived` actions

> NOTE: ENS does not offer "update subscription". We just simply delete and recreate new subscription.

## DEV

- DEV subscription use `development` project context
- Created as follows:
```
ica projects enter development

ica subscriptions create \
  --name "UMCCRGDSFilesEventDataPortalDevProject" \
  --type "gds.files" \
  --actions "uploaded,deleted,archived,unarchived" \
  --description "UMCCR data portal (DEV) subscribed to gds.files events using the development project" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<DEV_ACCOUNT_ID>/data-portal-ica-gds-event-queue" \
  --filter-expression "{\"or\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-primary-data-dev\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-run-data-dev\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-fastq-data-dev\"]},{\"and\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"development\"]},{\"or\":[{\"startsWith\":[{\"path\":\"$.path\"},\"/analysis_data/\"]},{\"startsWith\":[{\"path\":\"$.path\"},\"/primary_data/\"]}]}]}]}"
```

- Get:
```
ica subscriptions get sub.1910 -ojson | jq
```

## PROD

- PROD subscription use `production` project context
- Created as follows:
```
ica projects enter production

ica subscriptions create \
  --name "UMCCRGDSFilesEventDataPortalProdProject" \
  --type "gds.files" \
  --actions "uploaded,deleted,archived,unarchived" \
  --description "UMCCR data portal (PROD) subscribed to gds.files events using the production project" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<PROD_ACCOUNT_ID>/data-portal-ica-gds-event-queue" \
  --filter-expression "{\"or\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-primary-data-prod\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-run-data-prod\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-fastq-data-prod\"]},{\"and\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"production\"]},{\"or\":[{\"startsWith\":[{\"path\":\"$.path\"},\"/analysis_data/\"]},{\"startsWith\":[{\"path\":\"$.path\"},\"/primary_data/\"]}]}]}]}"
```

- Get:
```
ica subscriptions get sub.1921 -ojson | jq
```

## STG

- STG subscription use `staging` project context
- Created as follows:
```
ica projects enter staging

ica subscriptions create \
  --name "UMCCRGDSFilesEventDataPortalStgProject" \
  --type "gds.files" \
  --actions "uploaded,deleted,archived,unarchived" \
  --description "UMCCR data portal (STG) subscribed to gds.files events using the staging project" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<STG_ACCOUNT_ID>/data-portal-ica-gds-event-queue" \
  --filter-expression "{\"or\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-primary-data-stg\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-run-data-stg\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-fastq-data-stg\"]},{\"and\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"staging\"]},{\"or\":[{\"startsWith\":[{\"path\":\"$.path\"},\"/analysis_data/\"]},{\"startsWith\":[{\"path\":\"$.path\"},\"/primary_data/\"]}]}]}]}"
```

- Get:
```
ica subscriptions get sub.2478 -ojson | jq
```
