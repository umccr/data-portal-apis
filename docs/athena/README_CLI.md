# Athena CLI

_Athena query using AWS CLI_

> CONCEPT: Athena is a distributed query engine. Hence, unlike conventional SQL server request/response, the query result dispatch asynchronously. This is the typical case for BigData query processing. There are 3 parts in process from making a query to getting the result. As follows. 

https://awscli.amazonaws.com/v2/documentation/api/latest/reference/athena/index.html

## 0. Prelude

```
aws athena list-work-groups --profile prodops
```
```
aws athena list-data-catalogs --profile prodops
```
```
aws athena list-databases --catalog-name data_portal --profile prodops
```
```
aws athena get-database --catalog-name data_portal --database-name data_portal --profile prodops
```
```
aws athena list-named-queries --work-group data_portal --profile prodops
```
```
aws athena get-named-query --named-query-id 1e9c43b3-02e3-4822-9108-f461ac3b42d4 --profile prodops
```

## 1. Start Query Execution

> Say a use case, where we simply wish to extract some data points from [Portal Pipeline database](../model/data_portal_model.pdf) through Athena SQL Query.

- Here is an example [athena_cli_start.sh](athena_cli_start.sh) bash script that wrap AWS CLI Athena subcommand to make a query.

```
sh athena_cli_start.sh > athena_cli_start.json

cat athena_cli_start.json
{
    "QueryExecutionId": "f5da9315-76f1-47ee-9c38-bec1b44e60e9"
}
```

## 2. Poll Query Execution Status

```
aws athena get-query-execution --query-execution-id "f5da9315-76f1-47ee-9c38-bec1b44e60e9" --profile prodops
```

- Depends on the query execution status, you can do more polling. The state is SUCCEEDED but could be QUEUED, RUNNING, FAILED.

```
{
    "QueryExecution": {
        "QueryExecutionId": "f5da9315-76f1-47ee-9c38-bec1b44e60e9",
        "Query": "<SNIP>",
        "StatementType": "DML",
        "ResultConfiguration": {
            "OutputLocation": "s3://umccr-data-portal-build-prod/athena-query-results/f5da9315-76f1-47ee-9c38-bec1b44e60e9.csv",
            "EncryptionConfiguration": {
                "EncryptionOption": "SSE_S3"
            }
        },
        "QueryExecutionContext": {
            "Database": "data_portal",
            "Catalog": "data_portal"
        },
        "Status": {
            "State": "SUCCEEDED",
            "SubmissionDateTime": "2022-08-11T01:16:29.701000+10:00",
            "CompletionDateTime": "2022-08-11T01:16:37.291000+10:00"
        },
        "Statistics": {
            "EngineExecutionTimeInMillis": 6786,
            "DataScannedInBytes": 118029,
            "TotalExecutionTimeInMillis": 7590,
            "QueryQueueTimeInMillis": 521,
            "ServiceProcessingTimeInMillis": 283
        },
        "WorkGroup": "data_portal",
        "EngineVersion": {
            "SelectedEngineVersion": "AUTO",
            "EffectiveEngineVersion": "Athena engine version 2"
        }
    }
}
```

## 3. Download Query Result

```
aws s3 cp s3://umccr-data-portal-build-prod/athena-query-results/f5da9315-76f1-47ee-9c38-bec1b44e60e9.csv . --profile prodops
```

- Please note that the Athena result bucket has S3 life cycle policy that routinely purge away older query results. Please treat it as ephemeral data store. If this is of-concern, please kindly reach out.


## Next

See [Programmatic](./README.md) section for more advanced usage.
