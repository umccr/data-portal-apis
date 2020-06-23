import boto3

client = boto3.client("ssm")


def get_secret(key) -> str:
    """
    Retrieve the secret value from SSM.
    :param key: the key of the secret
    :return: the secret value
    """
    resp = client.get_parameter(
        Name=key,
        WithDecryption=True
    )
    return resp['Parameter']['Value']


def get_ssm_param(name):
    """
    Fetch the parameter with the given name from SSM Parameter Store.
    """
    return get_secret(name)


class SSMParamStore(object):
    def __init__(self, key):
        self._key = key
        self._value = None

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        assert self._key is not None, "Undefined key"
        return get_secret(self._key)

    def get_value(self):
        return self.value

    get: get_value

    def __str__(self):
        return f"{self._key}"
