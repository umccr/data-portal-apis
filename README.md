# UMCCR Data Portal APIs

---

## Deployment

`buildspec.yml` - build configuration for AWS CodeBuild

### Parameters

- STAGE: development stage (dev/prod)
- ATHENA_OUTPUT_LOCATION: the s3 bucket to store Athena output
- S3_KEYS_TABLE_NAME: name of the Athena table storing s3 keys data
- LIMS_TABLE_NAME: name of the Athena table storing LIMS data


## Local Testing

`mocks/*.json`: each file gives a test case for an API feature

### To invoke:
```
serverless invoke local --function delete --path mocks/file-search.json
```