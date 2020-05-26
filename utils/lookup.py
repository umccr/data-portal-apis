# -*- coding: utf-8 -*-
"""lookup module

Simple lookup table for id:name mapping
"""


def get_wg_name_from_id(wid):
    if wid == 'wid:e4730533-d752-3601-b4b7-8d4d2f6373de':
        return 'development'
    elif wid == 'wid:9c481003-f453-3ff2-bffa-ae153b1ee565':
        return 'collab-illumina-dev'
    elif wid == 'wid:acddbfda-4980-38ed-99fa-94fe79523959':
        return 'clinical-genomics-workgroup'
    elif wid == 'wid:4d2aae8c-41d3-302e-a814-cdc210e4c38b':
        return 'production'
    else:
        return 'unknown'


def get_aws_account_name(id_):
    if id_ == '472057503814':
        return 'prod'
    elif id_ == '843407916570':
        return 'dev (new)'
    elif id_ == '620123204273':
        return 'dev (old)'
    elif id_ == '602836945884':
        return 'agha'
    else:
        return id_
