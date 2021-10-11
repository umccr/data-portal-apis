# -*- coding: utf-8 -*-
"""libaws module

Factory module for creating boto3 client and resource to interface AWS services
"""
import boto3


def session(**kwargs):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

    :param kwargs:
    :return:
    """
    return boto3.session.Session(**kwargs)


def client(service_name, **kwargs):
    return session().client(service_name=service_name, **kwargs)


def resource(service_name, **kwargs):
    return session().resource(service_name=service_name, **kwargs)


def s3_client(**kwargs):
    return client('s3', **kwargs)


def sqs_client(**kwargs):
    return client('sqs', **kwargs)


def ssm_client(**kwargs):
    return client('ssm', **kwargs)


def sm_client(**kwargs):
    return client('secretsmanager', **kwargs)
