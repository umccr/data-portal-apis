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

### Setup Portal Token

- Follow setting up [Portal Token](PORTAL_TOKEN.md)
- Use appropriate Portal Token depending on PROD or DEV environment
- If you receive `Unauthorised` or similar then Portal Token has either expired or invalid token for target env.

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
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/lims?subject=SBJ00700" | jq
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
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/metadata?subject=SBJ00700" | jq
```

_Get Metadata record(s) by Subject, Assay Type, Phenotype:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/metadata?phenotype=tumor&type=wgs&subject=SBJ00700" | jq
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
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds?run=200612_A01052_0017_BH5LYWDSXY" | jq
```

_Search Fastq GDS files entries by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds?run=200612_A01052_0017_BH5LYWDSXY&search=.fastq.gz$" | jq
```

### Fastq Endpoint

_List Fastq entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq" | jq
```

_Get a Fastq record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq/35" | jq
```

_Get Fastq record(s) by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?run=200612_A01052_0017_BH5LYWDSXY" | jq
```

_Get Fastq record(s) by `rglb`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?rglb=L2000176" | jq
```

Similarly, you can filter request parameters on `rgid`, `rgsm`, `lane`.

### Sequence Endpoint

_List Sequence entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequence" | jq
```

_Get a Sequence record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequence/217" | jq
```

_Get Sequence record(s) by `name`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/sequence?name=200612_A01052_0017_BH5LYWDSXY" | jq
```

Similarly, you can filter request parameters on `run_id`, `instrument_run_id`, `status`.

### Workflows Endpoint

_Workflow is more to say "WorkflowRun"_

_List Workflow entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/workflows" | jq
```

_Get a Workflow record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/workflows/800" | jq
```

_Get Workflow record(s) by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/workflows?run=200612_A01052_0017_BH5LYWDSXY" | jq
```

Similarly, you can filter request parameters on `sample_name`, `type_name`, `end_status`.

## Experimental

### Reports Endpoint

_(*potentially large JSON response)_

_List Report entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/reports" | jq > reports.json
```

_Get a Report record by Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/reports?subject=SBJ00700" | jq > reports_SBJ00700.json
```

_Get a Report record by Subject, Report Type:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/reports?subject=SBJ00700&type=hrd_hrdetect" | jq
```
