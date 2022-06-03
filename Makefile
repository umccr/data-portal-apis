.EXPORT_ALL_VARIABLES:
DJANGO_SETTINGS_MODULE = data_portal.settings.local

install:
	@pip install -r requirements-dev.txt
	@yarn install
	@pre-commit install

check:
	@pre-commit run --all-files
	@npx yarn audit

up:
	@docker compose up -d

down:
	@docker compose down

stop:
	@docker compose down

ps:
	@docker compose ps

start:
	@pip install -r requirements-dev.txt
	@echo $$DJANGO_SETTINGS_MODULE
	@python manage.py migrate
	@python manage.py runserver_plus --print-sql --keep-meta-shutdown

.PHONY: syncdata, loaddata
syncdata:
	@. ./loaddata.sh; sync_db_dump

loaddata:
	@. ./loaddata.sh; copy_db_dump; load_db_dump; load_localstack

load_data: loaddata

reload:
	@. ./loaddata.sh; copy_db_dump; load_db_dump

load_localstack:
	@. ./loaddata.sh; load_localstack

test: load_localstack
	@pip install -r requirements-dev.txt
	@echo $$DJANGO_SETTINGS_MODULE
	@python manage.py migrate
	@python manage.py test

fastest: # fist run test!
	@python manage.py test data_processors.reports

sure:
	@python manage.py test data_processors.pipeline

mysql:
	@docker exec -it portal_db mysql -h 0.0.0.0 -D data_portal -u root -proot

migrate:
	@echo $$DJANGO_SETTINGS_MODULE
	@python manage.py migrate

test_iap_mock:
	@curl -s -H "Authorization: Bearer Test" -X GET http://localhost/v1/workflows/runs/wfr.anything_work | jq

test_localstack:
	@curl -s http://localhost:4566/health | jq

openapi:
	@curl -s http://localhost:8000/swagger.json | jq > swagger/swagger.json
