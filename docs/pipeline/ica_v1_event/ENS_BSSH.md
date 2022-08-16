# BSSH Events Subscription

> 🙋‍♂️ BSSH event subscriptions still use (now obsolete) "workgroup" context.

- BSSH sequence run event type denote `bssh.runs`
- We subscribe to `statuschanged` action

#### DEV 

- DEV subscription use `collab-illumina-dev` workgroup context
- Created as follows:
```
ica workgroups enter collab-illumina-dev

ica subscriptions create \
  --name "UMCCRBsshRunsDataPortalDev" \
  --type "bssh.runs" \
  --actions "statuschanged" \
  --description "UMCCR data portal (DEV) subscribe to bssh.runs statuschanged events using collab-illumina-dev workgroup" \
  --aws-sqs-queue "https://sqs.ap-southeast-2.amazonaws.com/<DEV_ACCOUNT_ID>/data-portal-dev-iap-ens-event-queue"
```

- List:
```
ica subscriptions list
ID      	NAME                      	TYPE     	ACTIONS
sub.1692	UMCCRBsshRunsDataPortalDev	bssh.runs	statuschanged
```

- Get:
```
ica subscriptions get sub.1692 -ojson | jq
```

#### PROD

_(ask-Flo-for-details)_
