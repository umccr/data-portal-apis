# -*- coding: utf-8 -*-
"""
Usage:
    pip install aws-requests-auth botocore
    export AWS_PROFILE=prodops
    python portal_api_sig4.py
"""
from urllib.parse import urlparse, ParseResult

import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

if __name__ == '__main__':
    obj: ParseResult = urlparse("https://api.data.prod.umccr.org/iam/lims")

    auth = BotoAWSRequestsAuth(
        aws_host=obj.hostname,
        aws_region="ap-southeast-2",
        aws_service="execute-api"
    )

    response = requests.get(obj.geturl(), auth=auth)

    resp_dict: dict = response.json()

    print(resp_dict)
