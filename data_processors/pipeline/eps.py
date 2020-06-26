# -*- coding: utf-8 -*-
"""Endpoint Point Service module
High level interfaces to endpoint services such as IAP
"""
import os

from libiap.openapi import libwes, libgds

from utils import libssm


class IAP(object):
    def __init__(self):
        self.iap_auth_token = os.getenv("IAP_AUTH_TOKEN", None)
        if self.iap_auth_token is None:
            self.iap_auth_token = libssm.get_secret(os.environ['SSM_KEY_NAME_IAP_AUTH_TOKEN'])
        self.iap_base_url = os.getenv("IAP_BASE_URL", "https://aps2.platform.illumina.com")


class GDSInterface(IAP):
    def __init__(self):
        super().__init__()
        configuration = libgds.Configuration(
            host=self.iap_base_url,
            api_key={
                'Authorization': self.iap_auth_token
            },
            api_key_prefix={
                'Authorization': "Bearer"
            },
        )
        self.api_client = libgds.ApiClient(configuration)


class WESInterface(IAP):
    def __init__(self):
        super().__init__()
        configuration = libwes.Configuration(
            host=self.iap_base_url,
            api_key={
                'Authorization': self.iap_auth_token
            },
            api_key_prefix={
                'Authorization': "Bearer"
            },
        )
        self.api_client = libwes.ApiClient(configuration)
