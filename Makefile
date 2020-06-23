up:
	@docker-compose up -d
	@docker-compose -f iap-mock.yml -p iap_mock up -d
	@pip install -r requirements-dev.txt
	@export DJANGO_SETTINGS_MODULE=data_portal.settings.local
	@python manage.py migrate
	@python manage.py runserver_plus --print-sql

test:
	@python manage.py test
