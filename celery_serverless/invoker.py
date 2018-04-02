# coding: utf-8
import os
import io
from pathlib import Path

import click
from ruamel.yaml import YAML
yaml = YAML()

from .cli_utils import run

CELERY_HANDLER_PATH = 'celery_serverless.handler_worker'


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

    return invoker(config)

def _infer_strategy(config):
    #if config['provider']['name'] == 'aws':
    #    return 'boto3'
    return 'serverless'


def _invoke_serverless(config, local=False):
    name = _get_lambda_name(config)
    command = 'serverless invoke'
    if local:
        command += ' local'

    command += ' --verbose --color --function %s' % name
    sio = io.BytesIO()
    for line, retcode in run(command, sio):
        click.echo(line, nl=False)
    if retcode != 0:
        raise RuntimeError('Command failed: %s' % command)


def _invoke_boto3(config):
    raise NotImplementedError('Use boto3 to invoke AWS Lambda')


def _get_lambda_name(config):
    for name, options in config['functions'].items():
        if options.get('handler') == CELERY_HANDLER_PATH:
            return name

    raise RuntimeError((
        "Handler '%s' not found on serverless.yml.\n"
        "Please fix it or run 'celery serverless init' to recreate one"
    ) % CELERY_HANDLER_PATH)
