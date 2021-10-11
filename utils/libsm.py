# -*- coding: utf-8 -*-
"""libsm module

Module interface with AWS Secrets Manager and, LRU cache for hit less to AWS endpoint if any
"""
import base64
import logging

from cachetools.func import lru_cache

from utils import libaws

logger = logging.getLogger(__name__)


@lru_cache(maxsize=64)
def get_secret(secret_name):

    resp = libaws.sm_client().get_secret_value(
        SecretId=secret_name
    )

    if 'SecretString' in resp:
        return resp['SecretString']
    else:
        return base64.b64decode(resp['SecretBinary'])
