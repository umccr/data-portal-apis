# User Guide

_Required:_ You will need [curl](https://curl.se/) and [jq](https://stedolan.github.io/jq/). On macOS, you can install like so: `brew install curl jq`

## Using Portal API

### Service Info

- [OpenAPI documentation available here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/umccr/data-portal-apis/dev/swagger/swagger.json)
- Portal Base URLs are as follows:
    - PROD: https://api.data.prod.umccr.org
    - DEV: https://api.data.dev.umccr.org
    - LOCAL: http://localhost:8000

### Setup Portal Token

- Follow setting up [Portal Token](PORTAL_TOKEN.md)

### LIMS Endpoint

_List LIMS entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/lims | jq
```

_Get a LIMS record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/lims/2866 | jq
```

### S3 Endpoint

_List S3 object entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/s3 | jq
```

_Get a S3 object record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/s3/309772 | jq
```

_Get PreSigned URL of this S3 object record:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/s3/309772/presign | jq
```

_List S3 object entries belongs to SBJ00700:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/s3?subject=SBJ00700 | jq
```

_List SBJ00700's BAMs S3 object entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/s3?subject=SBJ00700&search=.bam$ | jq
```

### Subjects Endpoint

_List Subject entries:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/subjects | jq
```

_Get a Subject:_
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" https://api.data.dev.umccr.org/subjects/SBJ00700 | jq
```
