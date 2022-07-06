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
- Please check AWS developer document [Control access for invoking an API](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-control-access-using-iam-policies-to-invoke-api.html) on tailoring the role permission in regard to your stack needs.
```
{
   "Effect": "Allow",
    "Action": [
        "execute-api:Invoke"
    ],
    "Resource": [
        "arn:aws:execute-api:<region>:<account-id>:<api-id>/<stage-name>/<HTTP-VERB>/<resource-path-specifier>"
    ]
}
```

... whereas

```
region                  = ap-southeast-2
account-id              = 123456789                           (depends on environment - DEV or PROD Account ID)
api-id                  = ssm://data_portal/backend/api_id    (depends on environment - avail at SSM parameter store or scan on target environment API Gateway resources with keyword "data-portal-api")
stage-name              = $default                            
HTTP-VERB               = GET or POST or ...                  (depends on caller - ref ENDPOINTS.md)
resource-path-specifier = lims or metadata or ...             (depends on caller - ref ENDPOINTS.md) 
```

... as an example strict `/metadata` endpoint `GET` query

```
{
   "Effect": "Allow",
    "Action": [
        "execute-api:Invoke"
    ],
    "Resource": [
        "arn:aws:execute-api:ap-southeast-2:123456789:abcdEFG123/$default/GET/metadata"
    ]
}
```

... or slightly relax on all endpoints as

```
{
   "Effect": "Allow",
    "Action": [
        "execute-api:Invoke"
    ],
    "Resource": [
        "arn:aws:execute-api:ap-southeast-2:123456789:abcdEFG123/*"
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


#### Caveats

- Unlike normal JWT authorized endpoint, the [GPL stack notes](https://github.com/umccr/gridss-purple-linx-nf/blob/5117e1793c183670e7e457999f8365b52069b3cd/deployment/lambdas/submit_job/lambda_entrypoint.py#L342-L345) that sudden special control characters are not working through `/iam/` counterpart. 


## Endpoints

See [endpoints.md](ENDPOINTS.md)
