# UMCCR Data Portal Backend API

Cloud native serverless backend API for [UMCCR](https://umccr.org) [Data Portal Client](https://github.com/umccr/data-portal-client).

## Local Development

#### TL;DR

- Recommended: **Python 3.8** and PyCharm IDE
- Create virtual environment and activate it, then:
```
source venv/bin/activate

aws sso login --profile dev && export AWS_PROFILE=dev

make up
docker ps
(depends on your laptop performance :P, please wait all services to be fully started)

make test_iap_mock
make start
```

- REST API at: http://localhost:8000
- MySQL Adminer at: http://localhost:8181  (u/p: data_portal)
    - You may also try [PyCharm Database explorer](https://www.jetbrains.com/help/pycharm/connecting-to-a-database.html)
    - You may also wish to try with [JetBrains DataGrip](https://www.jetbrains.com/datagrip/)
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

- If you are on MySQL Adminer, at Database list view press [`Refresh`](http://localhost:8181/?server=db&username=data_portal&refresh=1).

#### Husky & Git

> NOTE: [husky](.huskyrc.json) :dog2: will guard and enforce static code analysis such as `lint` and any security `audit` via pre-commit hook. You are encourage to fix those. If you wish to skip this for good reason, you can by-pass husky by using [`--no-verify`](https://github.com/typicode/husky/issues/124) flag in `git` command.

- The default branch is `dev`. Any merges are CI/CD to `DEV` account environment.
- The `master` branch is production. Any merges are CI/CD to `PROD` account environment.
- Merge to `master` should be fast-forward merge from `dev` to maintain sync and linearity as follow:
```
git checkout master
git merge --ff-only dev
git push origin master
```

## Portal Lambdas

```
aws sso login --profile dev
export AWS_PROFILE=dev
aws lambda invoke --function-name data-portal-api-dev-migrate output.json
aws lambda invoke --function-name data-portal-api-dev-lims_scheduled_update_processor output.json
```

> If lambda timeout error then try again. Lambda needs warm-up time and LIMS rows are growing.

## Serverless

> :hand: Above :point_up: sections are good enough for up and running Portal backend for local development purpose. You can take on Serverless and Deployment sections below for, when you want to extend some aspect of Portal backend REST API or lambda functions and, deploying of those features.

- First, install `serverless` CLI and its plugins dependencies:
```
node --version
v12.18.2

npm i -g yarn
yarn global add serverless
yarn install
```
- You can `serverless` invoke or deploy from local. But favour over [CodeBuild pipeline](buildspec.yml) for deploying into AWS dev/prod account environment.
- Serverless deployment targets only to AWS. Therefore, it needs to setup AWS environment specific variables by sourcing `start.sh` script as follows if you are planning to invoke/deploy directly from your local (laptop):
```
aws sso login --profile dev
export AWS_PROFILE=dev

source start.sh

serverless info --STAGE dev
serverless invoke -f migrate --STAGE dev --noinput
serverless invoke -f lims_scheduled_update_processor --STAGE dev --noinput
serverless deploy --STAGE dev
```

> If lambda timeout error then try again. Lambda needs warm-up time and LIMS rows are growing.

## Deployment

1. A **FRESH** deployment has to _first_ done with [Terraform Data Portal stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal), as IaC for longer-live infrastructure artifacts/services deployment.

2. Then, this API (shorter-live, a more repetitive backend stack) is provisioned by the Serverless framework (see [`serverless.yml`](serverless.yml)), within AWS CodeBuild and CodePipeline CI/CD build setup (see [`buildspec.yml`](buildspec.yml)) -- whereas **AWS specific environment variables** originated from `Terraform > CodeBuild > Serverless`.

## Destroy

- Before tear down [Terraform stack](https://github.com/umccr/infrastructure/tree/master/terraform/stacks/umccr_data_portal), it is required to run `serverless remove` to remove Lambda, API Gateway, API domain, ... resources created by this serverless stack.
- Example as follows:
```
aws sso login --profile dev
export AWS_PROFILE=dev

source start.sh

SLS_DEBUG=true serverless delete_domain --STAGE dev
SLS_DEBUG=true serverless remove --STAGE dev
```
