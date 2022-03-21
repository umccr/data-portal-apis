# User Guide

_Required:_ You will need [curl](https://curl.se/) and [jq](https://stedolan.github.io/jq/). On macOS, you can install like so: `brew install curl jq`

## Using Portal API

### Notes

> Design Note: Portal Internal IDs are not "stable"

- By design, Portal has modelled internal ID for each record that has sync-ed or ingested into Portal database.
- This ID is also known as "Portal Internal ID".
- When context is cleared, you can use this internal ID to retrieve the said record entity. e.g. List then Get.
- However, please do note that **these internal IDs are not "stable"** nor no guarantee globally unique.
- Portal may rebuild these IDs or change its schematic nature as it sees fit and/or further expansion.

### Service Info

- [OpenAPI documentation available here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/umccr/data-portal-apis/dev/swagger/swagger.json)
- API Base URLs are as follows:
    - PROD: `https://api.data.prod.umccr.org`
    - DEV: `https://api.data.dev.umccr.org`

## Authorization

Portal currently support 2 types of API authorization.
1. Portal Token
2. AWS IAM

### Portal Token

- Follow setting up [Portal Token](PORTAL_TOKEN.md)
- Use appropriate Portal Token depending on PROD or DEV environment
- If you receive `Unauthorised` or similar then Portal Token has either expired or invalid token for target env.
- Token valid for 24 hours (1 day)

### AWS IAM

> NOTE: AWS IAM is for those who have direct access to UMCCR AWS resources and, need to closely knit their solution within UMCCR AWS environment. And, re-using AWS SSO facility for accessing Portal APIs for conveniences. For one-off and out-of-band use cases, please use Portal Token with JWT OAuth flow.

- Basically, Portal APIs has mirrored `/iam/` prefix to all endpoints.
- Required: **AWS IAM credentials** or, assume role for **service-to-service** use case.
  - For local dev, this goes along with your AWS CLI setup.
  - For service user, you will need to add appropriate permission to "assume-role" policy (see below).
- Append Prefix: `/iam/` to the endpoint. For example:
```
https://api.data.prod.umccr.org/iam/lims
```
- You will then need to make [AWS Signature v4 singed request](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html) with AWS credentials to the Portal endpoints. There are readily available existing drop-in v4 signature signing library around. Some pointers are as follows.

#### Assume Role

- Required: Attach the following policy to the service role (IAM Role) permission.
```
{
   "Effect": "Allow",
    "Action": [
        "execute-api:Invoke"
    ],
    "Resource": [
        "*"
    ]
}
```

#### CLI

- Recommend to use [awscurl](https://github.com/okigan/awscurl) in-place of `curl` for IAM endpoints.
- As simplest case, you can wrap the `awscurl` and bash scripting it to query the Portal APIs; Then pipe to post-process with `jq` or any choice of text processor for transformation! 
- Example:
  - Install `brew install awscurl` 
  - Login AWS CLI using `ProdOperator` role as per normal
  - Then

_GET_
```
awscurl -H "Accept: application/json" --profile prodops --region ap-southeast-2 "https://api.data.prod.umccr.org/iam/lims" | jq
```

_POST_
```
awscurl -X POST -d '["220311_A01052_0085_AHGGTWDSX3"]' -H "Content-Type: application/json" --profile prodops --region ap-southeast-2 "https://api.data.prod.umccr.org/iam/pairing" | jq
```

#### Python

- Recommend to use [aws-requests-auth](https://github.com/davidmuller/aws-requests-auth) or [requests-aws4auth](https://github.com/tedder/requests-aws4auth)
- See [examples/portal_api_sig4.py](examples/portal_api_sig4.py)

#### Node.js

- Recommend to use [Amplify.Signer](https://aws-amplify.github.io/amplify-js/api/classes/signer.html) or [aws4](https://github.com/mhart/aws4) or [aws4-axios](https://github.com/jamesmbourne/aws4-axios)
- See [examples/portal_api_sig4.js](examples/portal_api_sig4.js)

#### R
- Recommend to use Python [requests-aws4auth](https://github.com/tedder/requests-aws4auth) through [reticulate](https://rstudio.github.io/reticulate/)
- See [examples/portal_api_sig4.R](examples/portal_api_sig4.R)

## Endpoints

### LIMS Endpoint

_List LIMS entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/lims" | jq
```

_Get a LIMS record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/lims/2866" | jq
```

_Get LIMS record(s) by Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/lims?subject_id=SBJ00700" | jq
```

_Get LIMS record(s) by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/lims?run=200612_A01052_0017_BH5LYWDSXY" | jq
```

_Search LIMS record(s) by Library ID:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/lims?search=L2000176" | jq
```

### Metadata Endpoint

_(aka Lab Metadata)_

_List Metadata entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/metadata" | jq
```

_Get a Metadata record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/metadata/10" | jq
```

_Get Metadata record(s) by Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/metadata?subject_id=SBJ00700" | jq
```

_Get Metadata record(s) by Subject, Assay Type, Phenotype:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/metadata?phenotype=tumor&type=wgs&subject_id=SBJ00700" | jq
```

### Subjects Endpoint

_List Subject entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/subjects" | jq
```

_Get a Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/subjects/SBJ00700" | jq
```

### S3 Endpoint

_List S3 object entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3" | jq
```

_Get a S3 object record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3/309772" | jq
```

_Get PreSigned URL of this S3 object record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3/309772/presign" | jq
```

_List S3 object entries belongs to SBJ00700:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3?subject=SBJ00700" | jq
```

_Search BAM S3 object entries by SBJ00700:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3?subject=SBJ00700&search=.bam$" | jq
```

### GDS Endpoint

_List GDS file entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds" | jq
```

_Get a GDS file record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds/10" | jq
```

_Get PreSigned URL of this GDS file record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds/10/presign" | jq
```

_List GDS file entries belongs to Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds?run=211125_A00130_0185_AHWC2HDSX2" | jq
```

_Search Fastq GDS files entries by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds?run=211125_A00130_0185_AHWC2HDSX2&search=.fastq.gz$" | jq
```

### Fastq Endpoint

_List Fastq entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq" | jq
```

_Get a Fastq record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq/1388" | jq
```

_Get Fastq record(s) by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?run=211014_A00130_0180_BHLGF7DSX2" | jq
```

_Get Fastq record(s) by Sequence Run & Project Owner:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?run=211014_A00130_0180_BHLGF7DSX2&project_owner=Bedoui" | jq
```

_Get Fastq record(s) by `rglb`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?rglb=L2101106" | jq
```

Similarly, you can filter request parameters on `rgid`, `rgsm`, `lane`. Additionally, `project_owner` from metadata.

### Sequence Endpoint

_List Sequence entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequence" | jq
```

_Get a Sequence record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequence/2" | jq
```

_Get Sequence record(s) by `instrument_run_id`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequence?instrument_run_id=211014_A00130_0180_BHLGF7DSX2" | jq
```

### SequenceRun Endpoint

_SequenceRun provide BSSH events timeline and transitions_

_List SequenceRun entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequencerun" | jq
```

_Get a SequenceRun record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequencerun/570" | jq
```

_Get SequenceRun record(s) by `instrument_run_id`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequencerun?instrument_run_id=211014_A00130_0180_BHLGF7DSX2" | jq
```

Similarly, you can filter request parameters on `run_id`, `status`.

### Workflows Endpoint

_List Workflow entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/workflows" | jq
```

_Get a Workflow record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/workflows/800" | jq
```

_Get Workflow by Sequence Run e.g. **BCL_CONVERT** workflow for a Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/workflows?type_name=bcl_convert&sequence_run__instrument_run_id=211129_A00130_0188_BHWCY3DSX2" | jq
```

Similarly, you can filter request parameters on `type_name`, `end_status`.

### LibraryRun Endpoint

_List LibraryRun entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/libraryrun" | jq
```

_Get a LibraryRun record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/libraryrun/33" | jq
```

_Get LibraryRun record(s) by `instrument_run_id`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/libraryrun?instrument_run_id=211014_A00130_0180_BHLGF7DSX2" | jq
```

Similarly, you can filter request parameters on `run_id`, `lane`.

### Reports Endpoint

_(*potentially large JSON response)_

_List Report entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/reports" | jq > reports.json
```

_Get a Report record by Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/reports?subject_id=SBJ01146" | jq > reports_SBJ01146.json
```

_Get a Report record by Subject, Report Type:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/reports?subject_id=SBJ01146&type=hrd_hrdetect" | jq > reports_SBJ01146_hrd_hrdetect.json
```

### Pairing Endpoint

_Create T/N Pairing by SequenceRuns:_
```
curl -s -X POST -d '["220311_A01052_0085_AHGGTWDSX3"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing" | jq
```

_In iam endpoint with awscurl:_
```
awscurl -X POST -d '["220311_A01052_0085_AHGGTWDSX3"]' -H "Content-Type: application/json" --profile prodops --region ap-southeast-2 "https://api.data.prod.umccr.org/iam/pairing" | jq
```

_POST payload JSON can also be in file as follows_:
```
curl -s -X POST -d "@pairing.json" -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing/by_sequence_runs" | jq
```

_Create T/N Pairing by Subjects:_
```
curl -s -X POST -d '["SBJ01031", "SBJ01032", "SBJ01033", "SBJ01034"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing/by_subjects" | jq
```

_Create T/N Pairing by Libraries:_
```
curl -s -X POST -d '["L2200331", "L2200332"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing/by_libraries" | jq
```

_Create T/N Pairing by Samples:_
```
curl -s -X POST -d '["PRJ220785", "PRJ220786"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing/by_samples" | jq
```

_Create T/N Pairing by Workflows (WGS QC wfr_id):_
```
curl -s -X POST -d '["wfr.7e52b7b957a140be9b11988355ab6fd1"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing/by_workflows" | jq
```

### Presign Endpoint

_POST [payload JSON file](files.json) that contain list of gds absolute path_:
```
curl -s -X POST -d "@files.json" -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/presign" | jq
```
