import logging
import os
from enum import Enum

from utils import libsm

logger = logging.getLogger(__name__)

ICA_BASE_URL = "https://aps2.platform.illumina.com"
ICA_TOKEN_SECRET_NAME = "IcaSecretsPortal"


class ENSEventType(Enum):
    """
    REF:
    https://support-docs.illumina.com/SW/ICA/Content/SW/ICA/ENS_AvailableEvents.htm
    """
    GDS_FILES = "gds.files"
    BSSH_RUNS = "bssh.runs"
    WES_RUNS = "wes.runs"


class GDSFilesEventType(Enum):
    """
    REF:
    https://support-docs.illumina.com/SW/ICA/Content/SW/ICA/ENS_AvailableEvents.htm
    """
    UPLOADED = "uploaded"
    DELETED = "deleted"
    ARCHIVED = "archived"
    UNARCHIVED = "unarchived"

    @classmethod
    def from_value(cls, value):
        if value == cls.UPLOADED.value:
            return cls.UPLOADED
        elif value == cls.DELETED.value:
            return cls.DELETED
        elif value == cls.ARCHIVED.value:
            return cls.ARCHIVED
        elif value == cls.UNARCHIVED.value:
            return cls.UNARCHIVED
        else:
            raise ValueError(f"No matching enum found for value: {value}")


def configuration(lib):
    ica_access_token = os.getenv("ICA_ACCESS_TOKEN", None)
    if ica_access_token is None:
        ica_access_token = libsm.get_secret(secret_name=ICA_TOKEN_SECRET_NAME)
    ica_base_url = os.getenv("ICA_BASE_URL", ICA_BASE_URL)

    config = lib.Configuration(
        host=ica_base_url,
        api_key={
            'Authorization': ica_access_token
        },
        api_key_prefix={
            'Authorization': "Bearer"
        },
    )

    # WARNING: only in local debug purpose, should never be committed uncommented!
    # it print stdout all libica.openapi http calls activity including JWT token in http header
    # config.debug = True

    return config
