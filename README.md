# UMCCR Data Portal Backend API

Cloud native serverless backend API for [UMCCR](https://umccr.org) [Data Portal Client](https://github.com/umccr/data-portal-client).

## Deployment

1. A fresh deployment has to _first_ done with [Terraform Data Portal stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal), as IaC for longer-live infrastructure artifacts/services deployment.

2. Then, this API (shorter-live, a more repetitive backend stack) is provisioned by the Serverless framework (`serverless.yml`), within AWS CodeBuild and CodePipeline CI/CD build setup (`buildspec.yml`) -- whereas **AWS specific environment variables** originated from `Terraform > CodeBuild > Serverless`.

## Development

#### Local Development

- Recommended: Python 3.8 and PyCharm IDE
- Create virtual environment and activate it, then:
```
aws sso login --profile=dev
export AWS_PROFILE=dev
make up
```
- REST API at: http://localhost:8000
- MySQL Adminer at: http://localhost:8181


#### Testing

- Run test suite
```commandline
make test
```

- Run individual test case, e.g.
```commandline
python manage.py test data_portal.tests.test_s3_object.S3ObjectTests.test_unique_hash
```

## Serverless

- You can serverless invoke or deploy from local dev. But favour over CodeBuild pipeline for deploying into production environment.
- Serverless deploy target only to AWS. Therefore need to setup AWS environment specific variables as follows:
```
aws sso login --profile=dev
export AWS_PROFILE=dev

terraform init .
source mkvar.sh dev

serverless info --STAGE dev
serverless deploy --STAGE dev
serverless invoke -f migrate --STAGE dev --noinput
serverless invoke -f lims_scheduled_update_processor --STAGE dev --noinput

(OR)

aws lambda invoke --function-name data-portal-api-dev-migrate output.json
aws lambda invoke --function-name data-portal-api-dev-lims_scheduled_update_processor output.json
```

## Destroy

- Before tear down [Terraform stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal), it is required to run `serverless remove` to remove Lambda, API Gateway, API domain, ... resources created by this serverless stack.
- Example as follows:
```
aws sso login --profile=dev
export AWS_PROFILE=dev

terraform init .
source mkvar.sh dev

SLS_DEBUG=true serverless delete_domain --STAGE dev
SLS_DEBUG=true serverless remove --STAGE dev
```
