import json

from django.core.serializers.json import DjangoJSONEncoder


def dumps(data: dict):
    return json.dumps(data, cls=DjangoJSONEncoder)
