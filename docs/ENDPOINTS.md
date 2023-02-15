# Portal Endpoints

### Service Info

- [OpenAPI documentation available here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/umccr/data-portal-apis/dev/swagger/swagger.json)

### LIMS Endpoint

_List LIMS entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/lims" | jq
```

_Get a LIMS record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/lims/2866" | jq
```

_Get LIMS record(s) by Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/lims?subject_id=SBJ00700" | jq
```

_Get LIMS record(s) by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/lims?run=200612_A01052_0017_BH5LYWDSXY" | jq
```

_Search LIMS record(s) by Library ID:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/lims?search=L2000176" | jq
```

### Metadata Endpoint

_(aka Lab Metadata)_

_List Metadata entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/metadata" | jq
```

_Get a Metadata record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/metadata/10" | jq
```

_Get Metadata record(s) by Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/metadata?subject_id=SBJ00700" | jq
```

_Get Metadata record(s) by Subject, Assay Type, Phenotype:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/metadata?phenotype=tumor&type=wgs&subject_id=SBJ00700" | jq
```

### Subjects Endpoint

_List Subject entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/subjects" | jq
```

_Get a Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/subjects/SBJ00700" | jq
```

### S3 Endpoint

_List S3 object entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/s3" | jq
```

_Get a S3 object record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/s3/309772" | jq
```

_Get PreSigned URL of this S3 object record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/s3/309772/presign" | jq
```

_List S3 object entries belongs to SBJ00700:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/s3?subject=SBJ00700" | jq
```

_Search BAM S3 object entries by SBJ00700:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/s3?subject=SBJ00700&search=.bam$" | jq
```

### GDS Endpoint

_List GDS file entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/gds" | jq
```

_Get a GDS file record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/gds/10" | jq
```

_Get PreSigned URL of this GDS file record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/gds/10/presign" | jq
```

_List GDS file entries belongs to Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/gds?run=211125_A00130_0185_AHWC2HDSX2" | jq
```

_Search Fastq GDS files entries by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/gds?run=211125_A00130_0185_AHWC2HDSX2&search=.fastq.gz$" | jq
```

### Presign Endpoint 

_(*This is for bulk signing GDS files)_

_POST [payload JSON file](files.json) that contain list of gds absolute path_:
```
curl -s -X POST -d "@files.json" -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/presign" | jq
```

### Fastq Endpoint

_List Fastq entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq" | jq
```

_Get a Fastq record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq/1388" | jq
```

_Get Fastq record(s) by Sequence Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?run=211014_A00130_0180_BHLGF7DSX2" | jq
```

_Get Fastq record(s) by Sequence Run & Project Owner:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?run=211014_A00130_0180_BHLGF7DSX2&project_owner=Bedoui" | jq
```

_Get Fastq record(s) by `rglb`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?rglb=L2101106" | jq
```

Similarly, you can filter request parameters on `rgid`, `rgsm`, `lane`. Additionally, `project_owner` from metadata.

### Sequence Endpoint

_List Sequence entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/sequence" | jq
```

_Get a Sequence record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/sequence/2" | jq
```

_Get Sequence record(s) by `instrument_run_id`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/sequence?instrument_run_id=211014_A00130_0180_BHLGF7DSX2" | jq
```

### SequenceRun Endpoint

_SequenceRun provide BSSH events timeline and transitions_

_List SequenceRun entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/sequencerun" | jq
```

_Get a SequenceRun record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/sequencerun/570" | jq
```

_Get SequenceRun record(s) by `instrument_run_id`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/sequencerun?instrument_run_id=211014_A00130_0180_BHLGF7DSX2" | jq
```

Similarly, you can filter request parameters on `run_id`, `status`.

### Workflows Endpoint

_List Workflow entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/workflows" | jq
```

_Get a Workflow record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/workflows/800" | jq
```

_Get Workflow by Sequence Run e.g. **BCL_CONVERT** workflow for a Run:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/workflows?type_name=bcl_convert&sequence_run__instrument_run_id=211129_A00130_0188_BHWCY3DSX2" | jq
```

Similarly, you can filter request parameters on `type_name`, `end_status`.

### LibraryRun Endpoint

_List LibraryRun entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/libraryrun" | jq
```

_Get a LibraryRun record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/libraryrun/33" | jq
```

_Get LibraryRun record(s) by `instrument_run_id`:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/libraryrun?instrument_run_id=211014_A00130_0180_BHLGF7DSX2" | jq
```

Similarly, you can filter request parameters on `run_id`, `lane`.

### Pairing Endpoint

_Create T/N Pairing by SequenceRuns:_
```
curl -s -X POST -d '["220311_A01052_0085_AHGGTWDSX3"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/pairing" | jq
```

_In iam endpoint with awscurl:_
```
awscurl -X POST -d '["220311_A01052_0085_AHGGTWDSX3"]' -H "Content-Type: application/json" --profile prodops --region ap-southeast-2 "https://api.portal.prod.umccr.org/iam/pairing" | jq
```

_POST payload JSON can also be in file as follows_:
```
curl -s -X POST -d "@pairing.json" -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/pairing/by_sequence_runs" | jq
```

_Create T/N Pairing by Subjects:_
```
curl -s -X POST -d '["SBJ01031", "SBJ01032", "SBJ01033", "SBJ01034"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/pairing/by_subjects" | jq
```

_Create T/N Pairing by Libraries:_
```
curl -s -X POST -d '["L2200331", "L2200332"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/pairing/by_libraries" | jq
```

_Create T/N Pairing by Samples:_
```
curl -s -X POST -d '["PRJ220785", "PRJ220786"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/pairing/by_samples" | jq
```

_Create T/N Pairing by Workflows (WGS QC wfr_id):_
```
curl -s -X POST -d '["wfr.7e52b7b957a140be9b11988355ab6fd1"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/pairing/by_workflows" | jq
```

### Somalier Endpoint

_Extract fingerprint for a BAM in GDS_
```
curl -s -X POST -d '{"gds_path": "gds://production/analysis_data/SBJ02296/wgs_tumor_normal/20220605e40c7f62/L2200674_L2200673_dragen/PRJ221207_tumor.bam"}' \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $PORTAL_TOKEN" \
    "https://api.portal.prod.umccr.org/somalier/extract" | jq
```

_Check fingerprint for a BAM in GDS_
```
curl -s -X POST -d '{"index": "gds://production/analysis_data/SBJ02296/wgs_tumor_normal/20220605e40c7f62/L2200674_L2200673_dragen/PRJ221207_tumor.bam"}' \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $PORTAL_TOKEN" \
    "https://api.portal.prod.umccr.org/somalier/check" | jq
```

_*Caveat: The AWS API Gateway has at most 30s timeout limit (see [Integration Timeout](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html)). Sometime somalier check might take beyond this limit. Hence, may receive `Internal Server Error`. For that, either try again (second time should be faster as warm-start) or, do try [Somalier Lambda](pipeline/automation/README.md#somalier) as alternate._
