import json
from typing import Any, Union

from django.core.serializers.json import DjangoJSONEncoder


def dumps(data: Any) -> str:
    return json.dumps(data, cls=DjangoJSONEncoder)


def loads(data: Union[str, bytes]) -> Any:
    return json.loads(data)
