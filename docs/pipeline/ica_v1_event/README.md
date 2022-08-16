# ICA v1 Event Subscription

- Data Portal uses ICA ENS -- [Event Notification Service](https://illumina.gitbook.io/ica-v1/events/e-overview) -- for workflow automation orchestration and GDS file indexing.

## TL;DR

_Current Subscriptions_

- [BSSH Event](ENS_BSSH.md)
- [GDS Event](ENS_GDS.md)
- [WES Event](ENS_WES.md)

## ICA CLI primer for managing ENS subscription

- ICA CLI use environment variable `ICA_ACCESS_TOKEN` if present in your current shell.

- Check that such token exist in your current shell:
```
env | grep ICA_ACCESS_TOKEN
```

- If it exists, clear it away:
```
unset ICA_ACCESS_TOKEN
```

- Login
```
ica login
```

- Initially, you are at your `personal` context. You can check like so:
```
ica tokens details
```

- Check any ENS subscriptions under your `personal` context
```
ica subscriptions list
```

- Enter into `development` project context
```
ica projects enter development
```

- Check UMCCR ENS subscriptions under `development` project context
```
ica subscriptions list | grep UMCCR
```

- Exit from `development` project context and back to your `personal` context
```
ica projects exit
```

- Enter into `production` project context
```
ica projects enter production
```

- Check UMCCR ENS subscriptions under `production` project context
```
ica subscriptions list | grep UMCCR
```

- Exit from `production` project context and back to your `personal` context
```
ica projects exit
```

## JSON Expression

- Event [JSON filter expression](https://illumina.gitbook.io/ica-v1/events/e-jsonexpressions) are coded in [expr.js](expr.js) for proper formatting.
- You can use the output and feed into `--filter-expression` flag, including double quote.  
- You need Node.js and can run like this, e.g:
```
node expr.js
"{\"or\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-primary-data-dev\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-run-data-dev\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-fastq-data-dev\"]}]}"
"{\"or\":[{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-primary-data-prod\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-run-data-prod\"]},{\"equal\":[{\"path\":\"$.volumeName\"},\"umccr-fastq-data-prod\"]}]}"
"{\"startsWith\":[{\"path\":\"$.WorkflowRun.Name\"},\"umccr__automated\"]}"
```
