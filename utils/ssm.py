import boto3

client = boto3.client('ssm')


def get_secret(key):
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
