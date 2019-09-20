from django.core.serializers.json import DjangoJSONEncoder
from django.http import JsonResponse


class JsonErrorResponse(JsonResponse):
    def __init__(self, errors: str, encoder=DjangoJSONEncoder, safe=True, json_dumps_params=None, **kwargs):
        super().__init__({'errors': errors}, encoder, safe, json_dumps_params, **kwargs)