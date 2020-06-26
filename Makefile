.EXPORT_ALL_VARIABLES:
DJANGO_SETTINGS_MODULE = data_portal.settings.local

up:
	@docker-compose up -d
	@docker-compose -f iap-mock.yml -p iap_mock up -d

down:
	@docker-compose down
	@docker-compose -f iap-mock.yml -p iap_mock down

start:
	@pip install -r requirements-dev.txt
	@echo $$DJANGO_SETTINGS_MODULE
	@python manage.py migrate
	@python manage.py runserver_plus --print-sql

test:
	@python manage.py test

migrate:
	@python manage.py migrate
