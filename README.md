# UMCCR Data Portal APIs

---

## Deployment

`buildspec.yml` - build configuration for AWS CodeBuild

### Parameters

- STAGE: development stage (dev/prod)
- ATHENA_OUTPUT_LOCATION: the s3 bucket to store Athena output
- S3_KEYS_TABLE_NAME: name of the Athena table storing S3 keys data
- LIMS_TABLE_NAME: name of the Athena table storing LIMS data


## Local Testing

Inside `mocks` directory, we can write `json` test cases. 

Example: a basic testcase for `file-search` API
```json
{
  "queryStringParameters": {
    "query": "csv key.include:csv pathinc:csv date:>2019-03-18 size:>1000",
    "page": 0,
    "sortCol": "last_modified_date",
    "sortAsc": "false",
  }
}
```

### To invoke:
```
serverless invoke local --function delete --path mocks/file-search.json
```