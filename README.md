# UMCCR Data Portal Backend API

Cloud native serverless backend API for [UMCCR](https://umccr.org) [Data Portal Client](https://github.com/umccr/data-portal-client).

[OpenAPI documentation available here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/umccr/data-portal-apis/dev/swagger/swagger.json). See [User Guide](docs) for API usage.

## Local Development

#### TL;DR

- Required:
  - **Python 3.9**
  - Node.js with Yarn
  - See `buildspec.yml` for runtime versions requirement
- Create virtual environment; use either built-in `python -mvenv venv` or [conda](https://docs.conda.io/en/latest/).

```
python -V
Python 3.9.6

node -v
v18.16.0

npm i -g yarn
yarn -v
3.5.1
```

then activate it:

```
source venv/bin/activate
(or)
conda activate myenv

(install Python and node.js development dependencies)
make install

aws sso login --profile dev && export AWS_PROFILE=dev

make up
make ps
(depends on your laptop performance :P, please wait all services to be fully started)

make test_iap_mock
make start
```

- REST API at: http://localhost:8000
- Swagger at: http://localhost:8000/swagger/
- ReDoc at: http://localhost:8000/redoc/
- Look into `Makefile` for more dev routine targets

#### Testing

- Run test suite
```commandline
make test
```

- Run individual test case, e.g.
```commandline
python manage.py test data_portal.tests.test_s3_object.S3ObjectTests.test_unique_hash
```

#### Loading Data

- You can sync the latest db dump from S3 bucket as follow:
```
aws sso login --profile dev && export AWS_PROFILE=dev
make syncdata
```

- Then, you can drop db and restore from db dump as follow:
```
make loaddata
```

#### Pre-commit Hook

> NOTE: We use [pre-commit](https://github.com/umccr/wiki/blob/master/computing/dev-environment/git-hooks.md). It will guard and enforce static code analysis such as `lint` and any security `audit` via pre-commit hook. You are encouraged to fix those. If you wish to skip this for good reason, you can by-pass [Git pre-commit hooks](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) by using `git commit --no-verify` flag.

```commandline
git config --unset core.hooksPath
pre-commit install
pre-commit run --all-files
```

#### GitOps

> NOTE: We use [GitOps](https://www.google.com/search?q=GitOps) and, release to deployment environments are all tracked by _long-running_ Git branches as follows.

- The default branch is `dev`. Any merges are CI/CD to `DEV` account environment.
- The staging branch is `stg`. Any merges are CI/CD to `STG` account environment.
- The `main` branch is production. Any merges are CI/CD to `PROD` account environment.

#### Git Flow

- Typically, make your feature branch out from `dev` to work on your story point. Then please submit PR to `dev`.
- Upon finalising release, create PR using GitHub UI from `dev` to `stg` or; from `stg` to `main` accordingly.

- Merge to `stg` should be fast-forward merge from `dev` to maintain sync and linearity as follows:
```
git checkout stg
git merge --ff-only dev
git push origin stg
```

- Merge to `main` should be fast-forward merge from `stg` to maintain sync and linearity as follows:
```
git checkout main
git merge --ff-only stg
git push origin main
```

## Portal Lambdas

```
aws sso login --profile dev
export AWS_PROFILE=dev
aws lambda invoke --function-name data-portal-api-dev-migrate output.json
```

## Serverless

> Above sections are good enough for up and running Portal backend for local development purpose. You can take on Serverless and Deployment sections below for, when you want to extend some aspect of Portal backend REST API or lambda functions and, deploying of those features.

- First, install `serverless` CLI and its plugins dependencies:
```
yarn install
npx serverless --version
```

- You can `serverless` invoke or deploy from local. However, we favour [CodeBuild pipeline](buildspec.yml) for deploying into AWS dev/prod account environment.
- Serverless deployment targets only to AWS. AWS account specific variables will be loaded from SSM Parameter Store of respective login session:
```
aws sso login --profile dev && export AWS_PROFILE=dev

npx serverless info --stage dev
npx serverless invoke -f migrate --stage dev
npx serverless invoke -f lims_scheduled_update_processor --stage dev
npx serverless deploy --stage dev --debug='*' --verbose
```

## Deployment

1. A **FRESH** deployment has to _first_ done with [Terraform Data Portal stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal), as IaC for longer-live infrastructure artifacts/services deployment.

2. Then, this API (shorter-live, a more repetitive backend stack) is provisioned by the Serverless framework (see [`serverless.yml`](serverless.yml)), within AWS CodeBuild and CodePipeline CI/CD build setup (see [`buildspec.yml`](buildspec.yml)) -- whereas **AWS specific environment variables** originated from `Terraform > CodeBuild > Serverless`, if any.

## Destroy

- Before tear down [Terraform stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal), it is required to run `serverless remove` to remove Lambda, API Gateway, API domain, ... resources created by this serverless stack.
- Example as follows:
```
aws sso login --profile dev && export AWS_PROFILE=dev

npx serverless delete_domain --stage dev
npx serverless remove --stage dev
```

## X-Ray

> X-Ray SDK is disabled by default!

- Portal API backend and data processing functions can be traced with [X-Ray instrumentation](https://docs.aws.amazon.com/lambda/latest/dg/services-xray.html).
- You can enable X-Ray SDK by setting the [Lambda Configuration Environment variable](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html) in **each** Portal Lambda function, e.g.

```
  ...
  ...
  "Environment": {
    "Variables": {
      "DJANGO_SETTINGS_MODULE": "data_portal.settings.aws",
      "AWS_XRAY_SDK_ENABLED": "true"
    }
  },
  "TracingConfig": {
    "Mode": "Active"
  },
  ...
  ...
```

- You can observe deployed Lambda functions as follows:

```
aws sso login --profile dev && export AWS_PROFILE=dev

aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `data-portal-api`) == `true`].FunctionName'

aws lambda list-functions | jq '.Functions[] | select(.FunctionName == "data-portal-api-dev-sqs_s3_event_processor")'
```

- You can then use [AWS Lambda Console](https://console.aws.amazon.com/lambda) to enable `AWS_XRAY_SDK_ENABLED` to `true`.
- While at AWS Lambda Console, you must also turn on: **Configuration** > **Monitoring and operations tools** > [**Active tracing**](https://docs.aws.amazon.com/xray/latest/devguide/xray-services-lambda.html).
- Then make few Lambda invocations, and you can use [AWS X-Ray Console](https://console.aws.amazon.com/xray/home) > **Traces** to start observe tracing.
- Please switch off the setting back when no longer in use.

### Segments

- By default, X-Ray SDK support auto instrumentation (i.e. auto created segments) for Django framework, including database queries, rendering subsegments, etc.
- You can however acquire the current `segment` elsewhere in program code as follows:

```
segment = xray_recorder.current_segment()
```

- Or you can add `subsegment` to start [trace from your Lambda handler](https://github.com/aws/aws-xray-sdk-python#trace-aws-lambda-functions) entrypoint:

```
from aws_xray_sdk.core import xray_recorder

def lambda_handler(event, context):
    # ... some code

    subsegment = xray_recorder.begin_subsegment('subsegment_name')
    # Code to record
    # Add metadata or annotation here, if necessary
    subsegment.put_metadata('key', dict, 'namespace')
    subsegment.put_annotation('key', 'value')

    xray_recorder.end_subsegment()

    # ... some other code
```

- Refer the following links for example and doc:
  - https://docs.aws.amazon.com/xray-sdk-for-python/latest/reference/index.html
  - https://docs.aws.amazon.com/lambda/latest/dg/python-tracing.html
