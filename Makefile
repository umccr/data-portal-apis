up:
	@docker-compose up -d
	@pip install -r requirements-dev.txt
	@export DJANGO_SETTINGS_MODULE=data_portal.settings.local
	@python manage.py runserver_plus --print-sql

test:
	@python manage.py test
