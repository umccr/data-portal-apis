# UMCCR Data Portal Backend API

Cloud native serverless backend API for [UMCCR](https://umccr.org) [Data Portal Client](https://github.com/umccr/data-portal-client).

A fresh deployment has to _first_ done with [Terraform Data Portal stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal).

Then, this stack is provisioned by the Serverless framework (`serverless.yml`), within AWS CodeBuild/CodePipeline environment (`buildspec.yml`), where environment variables originated from Terraform > CodeBuild to Serverless.

## Stack Overview

#### Lambda functions
- `api` (available through API Gateway): 
  - s3 file search
  - s3 url signing
- `sqs_s3_event_processor`: handles S3 create/update/delete events, through SQS (which is provisioned by
  Terraform)
- `lims_update_processor`: handles LIMS spreadsheet update
  - Note that if we want to rewrite LIMS data (where we also have production-scale s3 objects data in the db), 
    we will need to run `data_processors/lims_rewrite_processor.py` (or Django management command `lims_rewrite` 
    for custom csv file S3 location) in a separate place such as an EC2 environment,
    which should be in the same VPC and Security Group as the lambda functions.
- `migrate`: used for updating db schema, run at the end of serverless deployment 
   (see `deploy:finalize` in `serverless.yml`)

#### API
- Custom API domain
  - The domain name is given by Terraform
- WAF (Web Application Firewall)
  - Associated with the WAF provisioned by Terraform


## Development

#### Python & Django directory structure
- `data_portal/`: use this as the Django project root for IDEs such as PyCharm
  - `management/commands/`: custom commands, currently only have `lims_rewrite_processor` as mentioned above
  - `migrations/`: auto-generated (by Django) db migration files
  - `tests/`: unit tests for scripts in `data_portal` package
- `data_processors/`: where all data processors reside in. 
  - `tests`: unit tests for all these data processing functions
- `utils/`: utility functions

#### Local Development
- With in virtualenv for Python, we can do things like `manage.py xxx`.
- We can also use `serverless invoke` to invoke our lambda functions locally:
  - `serverless invoke local --function {function_name} --STAGE dev --path {mock_file_path}.json`
- As the stack is dependent on a number of env variables, we need to manually
  export them in local environment. We can simply use the CAPITAL_LETTER_ONES (and their values) in Terraform output.

#### Testing
- Integration tests are available at different places (and we can do `python manage.py test {package}` to run these
  tests.
- Integration tests are integrated as part of the CodeBuild. (See `buildspec.yml`)
- As AWS services are involved in the workflow of some functions, so for testing
 `moto` is used for mocking these services.
 
## Destroy
 
* Before tear down Terraform stack, it is required to run `serverless remove` to tear down application Lambda, API Gateway, API domain, and S3 buckets, ... i.e. resources created by this serverless stack, _manually_ (contrast to serverless deploy done through CodeBuild/CodePipeline in CI/CD fashion). 
* First, `terraform output` and export all UPPERCASE environment variables.
* Then, run `serverless delete_domain && serverless remove` to tear down the stack.
* Example as follows:
```
ssoawsdev
export AWS_PROFILE=dev

cd {...}/umccr/infrastructure/terraform/stacks/umccr_data_portal
terraform workspace select dev
terraform output
export API_DOMAIN_NAME=<value>
export CERTIFICATE_ARN=<value>
export LAMBDA_IAM_ROLE_ARN=<value>
export LAMBDA_SECURITY_GROUP_IDS=<value>
export LAMBDA_SUBNET_IDS=<value>
export LIMS_BUCKET_NAME=<value>
export LIMS_CSV_OBJECT_KEY=<value>
export S3_EVENT_SQS_ARN=<value>
export SSM_KEY_NAME_DJANGO_SECRET_KEY=<value>
export SSM_KEY_NAME_FULL_DB_URL=<value>
export WAF_NAME=<value>

cd {...}/umccr/data-portal/data-portal-apis
SLS_DEBUG=true serverless deploy list --STAGE dev
SLS_DEBUG=true serverless info --STAGE dev

aws s3 ls | grep serverless
aws apigateway get-rest-apis
aws lambda list-functions | grep portal
aws cloudformation list-stacks | grep portal

(if all good then)
SLS_DEBUG=true serverless delete_domain --STAGE dev
SLS_DEBUG=true serverless remove --STAGE dev
```
