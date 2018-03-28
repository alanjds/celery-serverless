# coding: utf-8
import os
from pathlib import Path

from ruamel.yaml import YAML
yaml = YAML()


def invoke_main(strategy=''):
    local_serverless = './serverless.yml'
    if not os.path.exists(local_serverless):
        raise RuntimeError("No serverless.yml detected. Please run 'celery serverless init' to create one")
    config = yaml.load(Path(local_serverless))

    if not strategy:
        strategy = _infer_strategy(config)

    if strategy == 'serverless':
        invoker = _invoke_serverless
    elif strategy == 'boto3':
        invoker = _invoke_boto3
    else:
        raise NotImplementedError("Could not find a way to invoke via '%s' strategy" % strategy)

    return invoker()


def _infer_strategy(config):
    if config['provider']['name'] == 'aws':
        return 'boto3'
    return 'serverless'


def _invoke_serverless():
    raise NotImplementedError('Call: sls invoke ...')


def _invoke_boto3():
    raise NotImplementedError('Use boto3 to invoke AWS Lambda')
