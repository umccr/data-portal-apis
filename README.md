# UMCCR Data Portal Backend API

Cloud native serverless backend API for [UMCCR](https://umccr.org) [Data Portal Client](https://github.com/umccr/data-portal-client).

A fresh deployment has to _first_ done with [Terraform Data Portal stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal).

Then, this stack is provisioned by the Serverless framework (`serverless.yml`), within AWS CodeBuild and CodePipeline environment (`buildspec.yml`), where environment variables originated from Terraform > CodeBuild > Serverless.

## Development

#### Local Development

- Required Python 3.8 (see [.python-version](.python-version)) and recommended PyCharm IDE

```
docker-compose up -d
pip install -r requirements-dev.txt
export DJANGO_SETTINGS_MODULE=data_portal.settings.local
python manage.py runserver_plus --print-sql
```

- http://localhost:8000
- http://localhost:8181


#### Testing

- Run test suite
```
python manage.py test
```

- Run individual test case, e.g.
```
python manage.py test data_portal.tests.test_s3_object.S3ObjectTests.test_unique_hash
```

#### Serverless

- You can serverless invoke or deploy from local dev. But favour over CodeBuild pipeline for deploying into production environment.
- Serverless deploy target only to AWS. Therefore need to setup AWS environment specific variables as follows:
```
ssoawsdev
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
ssoawsdev
export AWS_PROFILE=dev

terraform init .
source mkvar.sh dev

SLS_DEBUG=true serverless delete_domain --STAGE dev
SLS_DEBUG=true serverless remove --STAGE dev
```
