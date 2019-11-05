# UMCCR Data Portal Back End

The stack is provisioned by the Serverless framework (`serverless.yml`),
within AWS CodeBuild environment (`buildspec.yml` is the configuration file), where environment
variables originated from Terraform > CodeBuild to Serverless.

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