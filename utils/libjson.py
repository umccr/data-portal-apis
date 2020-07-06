import json

from django.core.serializers.json import DjangoJSONEncoder


def dumps(data: dict) -> str:
    return json.dumps(data, cls=DjangoJSONEncoder)


def loads(data: str) -> dict:
    return json.loads(data)
