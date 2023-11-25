# RNAsum ICA Pipeline Lambda

## Notes:

- RNAsum in ICA Pipeline is fully automated. 
- However, only with statically configured TCGA dataset that set in RNAsum workflow input template.
- For dynamic lookup, see issue [#417](https://github.com/umccr/data-portal-apis/issues/417).

The following options are available at the moment. 

> NOTE: Options 1 and 2 will use _latest-greatest_ strategy 
> i.e. It will use most recent umccrise and transcriptome outputs of recently sequenced library, respectively.

## Option 1

Required:

- Subject ID
- TCGA Dataset

Implicit Required:

- At least, 1 run of umccrise workflow that must be completed for the related WGS libraries 
- At least, 1 run of transcriptome (`wts_tumor_only`) workflow that must be completed for the related WTS tumor library.

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-rnasum_by_subject \
  --cli-binary-format raw-in-base64-out \
  --payload '{
        "subject_id": "SBJ00001",
        "dataset": "BRCA"  
     }' \
  rnasum_by_subject_lambda_output.json
```

## Option 2

Required:

- Umccrise workflow run ID (that must be completed through Portal ICA Pipeline)
- TCGA Dataset

Implicit Required:

- At least, 1 run of transcriptome (`wts_tumor_only`) workflow must be completed for the related WTS tumor library.

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-rnasum_by_umccrise \
  --cli-binary-format raw-in-base64-out \
  --payload '{
        "wfr_id": "wfr.<umccrise_wfr_id>",
        "dataset": "BRCA"  
     }' \
  rnasum_by_umccrise_lambda_output.json
```

## Option 3

We need to prepare [RNAsum Lambda payload](https://github.com/umccr/data-portal-apis/blob/dev/data_processors/pipeline/lambdas/rnasum.py#L76-L95).

Required:

- Umccrise workflow output location
- Transcriptome (`wts_tumor_only`) workflow output location
- Sample name (e.g. `PRJ000012`)
- Report directory output (e.g. `SBJ00001__L0000001`)
- TCGA dataset (e.g. `BRCA`)
- Subject ID (e.g. `SBJ00001`)
- WTS tumor library ID (Please remove `_topup` or `_rerun` suffixes, if any. We merged them with original Library ID. e.g. `L0000001`)

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-rnasum \
  --cli-binary-format raw-in-base64-out \
  --payload '{
        "dragen_transcriptome_directory": {
            "class": "Directory",
            "location": "gds://path/to/WTS/output/dir"
        },
        "arriba_directory": {
            "class": "Directory",
            "location": "gds://path/to/arriba/output/dir"
        },
        "umccrise_directory": {
            "class": "Directory",
            "location": "gds://path/to/umccrise/output/dir"
        },
        "sample_name": "TUMOR_SAMPLE_ID",
        "report_directory": "SUBJECT_ID__WTS_TUMOR_LIBRARY_ID",
        "dataset": "reference_data",
        "subject_id": "SUBJECT_ID",
        "tumor_library_id": "WTS_TUMOR_LIBRARY_ID"  
     }' \
  rnasum_lambda_output.json
```

## Logs

The respective Lambda logs can be viewed at [CloudWatch log groups](https://ap-southeast-2.console.aws.amazon.com/cloudwatch/home?region=ap-southeast-2#logsV2:log-groups) in PROD:

```
/aws/lambda/data-portal-api-prod-rnasum_by_subject
/aws/lambda/data-portal-api-prod-rnasum_by_umccrise
/aws/lambda/data-portal-api-prod-rnasum
```
