from setuptools import setup, find_packages

setup(
    name="data-portal-apis",
    version="0.1.0",
    packages=find_packages(),
    url="https://github.com/umccr/data-portal-apis",
    author="UMCCR",
    description="UMCCR Data Portal APIs",
    extras_require={
        "dev": [
            "Werkzeug",
            "pipdeptree",
            "django_extensions",
        ],
        "test": [
            "moto==1.3.14",
            "factory_boy==2.12.0",
            "pytz==2019.3",
        ],
    },
    install_requires=[
        "aws-xray-sdk==2.4.3",
        "boto3==1.12.22",
        "botocore==1.15.22",
        "Django==3.0.4",
        "django-environ==0.4.5",
        "django-cors-headers==3.2.1",
        "djangorestframework==3.11.0",
        "drf-nested-routers==0.91",
        "pymysql==0.9.3",
        "python-dateutil==2.8.1",
        "google-api-python-client==1.8.0",
        "google-auth==1.11.3",
    ],
)
