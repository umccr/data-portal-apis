# Portal IAM

> NOTE: AWS IAM is for those who have access to UMCCR AWS and, need to closely knit their solution within UMCCR AWS environment. It can also reuse your AWS CLI SSO session for accessing Portal APIs. For one-off and out-of-band use cases, please use Portal Token.

- Basically, Portal APIs has mirrored `/iam/` prefix to all endpoints.
- Required: **AWS IAM credentials** or, assume role for **service-to-service** use case.
  - For local dev, this goes along with your AWS CLI v2 SSO setup.
  - For service user, you will need to add appropriate permission to "assume-role" policy to your stack (see below).
- Append Prefix: `/iam/` to the endpoint. For example:
```
https://api.portal.prod.umccr.org/iam/lims
```
- You will then need to make [AWS Signature v4 singed request](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html) with AWS credentials to the Portal endpoints. There are readily available existing drop-in v4 signature signing library around. Some pointers are as follows.

## CLI

As simplest case, you can wrap the  `curl` or `awscurl` and, bash scripting it to query the Portal APIs. Then, pipe to post-process with `jq` (or any choice of text processor) for post-processing and transformation!

### curl

From [Peter's note](https://umccr.slack.com/archives/CCC5J2NM6/p1690169029051099?thread_ts=1689556612.584249&cid=CCC5J2NM6):

```
curl --request GET \
  "https://api.portal.prod.umccr.org/iam/lims?subject_id=SBJ01651" \
  --aws-sigv4 "aws:amz:${AWS_REGION}:execute-api" \
  --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" \
  --header "x-amz-security-token: ${AWS_SESSION_TOKEN}" \
  --header 'Accept: application/json'
```

### awscurl

From Patto's pointer:

- You can use [awscurl](https://github.com/okigan/awscurl) for IAM endpoints.
- Example:
  - Install `brew install awscurl` 
  - Login AWS CLI SSO
  - Then

_GET_
```
awscurl -H "Accept: application/json" --profile prodops --region ap-southeast-2 "https://api.portal.prod.umccr.org/iam/lims" | jq
```

_POST_
```
awscurl -X POST -d '["220311_A01052_0085_AHGGTWDSX3"]' -H "Content-Type: application/json" --profile prodops --region ap-southeast-2 "https://api.portal.prod.umccr.org/iam/pairing" | jq
```


## Programmatic

### Python

- Recommend to use [aws-requests-auth](https://github.com/davidmuller/aws-requests-auth) or [requests-aws4auth](https://github.com/tedder/requests-aws4auth)
- See [examples/portal_api_sig4.py](examples/portal_api_sig4.py)

### Node.js

- Recommend to use [Amplify.Signer](https://aws-amplify.github.io/amplify-js/api/classes/signer.html) or [aws4](https://github.com/mhart/aws4) or [aws4-axios](https://github.com/jamesmbourne/aws4-axios)
- See [examples/portal_api_sig4.js](examples/portal_api_sig4.js)

### R
- Recommend to use Python [requests-aws4auth](https://github.com/tedder/requests-aws4auth) through [reticulate](https://rstudio.github.io/reticulate/)
- See [examples/portal_api_sig4.R](examples/portal_api_sig4.R)


### Caveats

- Unlike normal JWT authorized endpoint, the [GPL stack notes](https://github.com/umccr/gridss-purple-linx-nf/blob/5117e1793c183670e7e457999f8365b52069b3cd/deployment/lambdas/submit_job/lambda_entrypoint.py#L342-L345) that sudden special control characters are not working through `/iam/` counterpart. 


## Service User

### Assume Role

- Required: Attach the following policy to the service role (IAM Role) permission in your stack.
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
account-id              = 123456789                           (depends on environment - DEV or STG or PROD Account ID)
api-id                  = ssm://data_portal/backend/api_id    (depends on environment - see below)
stage-name              = $default                            
HTTP-VERB               = GET or POST or ...                  (depends on caller - ref ENDPOINTS.md)
resource-path-specifier = lims or metadata or ...             (depends on caller - ref ENDPOINTS.md) 
```

... for `api-id`, get SSM parameter
```
aws ssm get-parameter --name '/data_portal/backend/api_id' --output json --profile <dev|stg|prod> | jq '.Parameter.Value'
```

... as an example strict configuration for `/metadata` endpoint `GET` query only

```
{
   "Effect": "Allow",
    "Action": [
        "execute-api:Invoke"
    ],
    "Resource": [
        "arn:aws:execute-api:ap-southeast-2:<account-id>:<api-id>/$default/GET/metadata"
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
        "arn:aws:execute-api:ap-southeast-2:<account-id>:<api-id>/*"
    ]
}
```
