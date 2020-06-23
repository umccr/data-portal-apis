# -*- coding: utf-8 -*-
"""Endpoint Point Service module
High level interfaces to endpoint services such as IAP
"""
import os

from libiap.openapi import libwes, libgds

from utils import libssm

iap_auth_token = os.getenv("IAP_AUTH_TOKEN", None)
if iap_auth_token is None:
    iap_auth_token = libssm.get_secret(os.environ['SSM_KEY_NAME_IAP_AUTH_TOKEN'])

iap_base_url = os.getenv("IAP_BASE_URL", "https://aps2.platform.illumina.com")


class GDSInterface(object):
    configuration = libgds.Configuration(
        host=iap_base_url,
        api_key={
            'Authorization': iap_auth_token
        },
        api_key_prefix={
            'Authorization': "Bearer"
        },
    )

    api_client = libgds.ApiClient(configuration)


class WESInterface(object):
    configuration = libwes.Configuration(
        host=iap_base_url,
        api_key={
            'Authorization': iap_auth_token
        },
        api_key_prefix={
            'Authorization': "Bearer"
        },
    )
    api_client = libwes.ApiClient(configuration)
