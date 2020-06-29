# -*- coding: utf-8 -*-
"""libslack module

Module interface for underlay Slack client operations
Loosely based on design patterns: Facade, Adapter/Wrapper

Should retain/suppress all Slack API calls here, including
Slack API specific exceptions and data type that need for processing response.

Impl Note:

There are couple of ways to send Slack messages:
https://api.slack.com/messaging/sending#sending_methods

#1
For Slack Web API and Real Time Messaging (RTM) API, should utilise `python-slackclient`:
https://github.com/slackapi/python-slackclient

#2
For Slack web hook, apparently there is no official Python SDK[1] for Incoming Webhooks[2].
We could leverage package like `slack-webhook`[3]. However, we will be using built-in
`http.client` for now for back-porting existing code[4][5] purpose.

[1]: https://slack.dev
[2]: https://api.slack.com/messaging/webhooks
[3]: https://pypi.org/project/slack-webhook/
[4]: https://github.com/umccr/infrastructure/blob/master/cdk/apps/slack/lambdas/iap/notify_slack.py
[5]: https://github.com/umccr/infrastructure/blob/master/terraform/stacks/bootstrap/lambdas/notify_slack.py

If unsure, start with Pass-through call.
"""
import http.client
import logging
import os
from enum import Enum
from http import HTTPStatus
from typing import List

from utils import libssm, libjson

headers = {
    'Content-Type': 'application/json',
}

logger = logging.getLogger(__name__)

DEFAULT_SLACK_WEBHOOK_SSM_KEY = "/slack/webhook/id"


def call_slack_webhook(sender, topic, attachments, ssm=True, **kwargs):
    if ssm:
        ssm_key = kwargs.get('ssm_key', None)
        if ssm_key is None:
            ssm_key = DEFAULT_SLACK_WEBHOOK_SSM_KEY
        slack_webhook_id = libssm.get_ssm_param(ssm_key)
    else:
        slack_webhook_id = kwargs.get('webhook_id', None)
        if slack_webhook_id is None:
            slack_webhook_id = os.getenv('SLACK_WEBHOOK_ID', None)

    assert slack_webhook_id is not None, "SLACK_WEBHOOK_ID is not defined"

    slack_webhook_endpoint = f"/services/{slack_webhook_id}"

    slack_host = kwargs.get('host', None)
    if slack_host is None:
        slack_host = os.getenv('SLACK_HOST', None)
    if slack_host is None:
        slack_host = "hooks.slack.com"

    slack_channel = kwargs.get('channel', None)
    if slack_channel is None:
        slack_channel = os.getenv('SLACK_CHANNEL', None)

    assert slack_channel is not None, "SLACK_CHANNEL is not defined"

    connection = http.client.HTTPSConnection(slack_host)

    post_data = {
        "channel": slack_channel,
        "username": sender,
        "text": f"*{topic}*",
        "icon_emoji": ":aws_logo:",
        "attachments": attachments
    }
    logger.info(f"Slack POST data: {libjson.dumps(post_data)}")

    connection.request("POST", slack_webhook_endpoint, libjson.dumps(post_data), headers)
    response = connection.getresponse()
    logger.info(f"Slack webhook response status: {response.status}")
    connection.close()

    # see https://api.slack.com/messaging/webhooks#handling_errors
    if response.status == HTTPStatus.OK:
        return response.status
    else:
        return None


class SlackColor(Enum):
    GREEN = "#36a64f"
    RED = "#ff0000"
    BLUE = "#439FE0"
    GRAY = "#dddddd"
    BLACK = "#000000"


class SlackField(object):
    pass


class SlackAttachment(object):
    pass


class SlackMessage(object):
    def __init__(self, sender, topic, attachments):
        self.sender: str = sender
        self.topic: str = topic
        self.attachments: List[SlackAttachment] = attachments
