# coding: utf-8
import logging

import click
from celery_serverless.config import get_config


logger = logging.getLogger(__name__)

try:
    import boto3
    import botocore
    try:
        lambda_client = boto3.client('lambda')
    except botocore.exceptions.NoRegionError:
        logger.warning("'boto3' invoker cannot be used: please set a default region on serverless.yml")
        lambda_client = None
except ImportError:  # Boto3 is an optional extra on setup.py
    lambda_client = None

from .cli_utils import run



CELERY_HANDLER_PATH = 'celery_serverless.handler_worker'


def invoke_main(strategy=''):
    config = get_config()
    if not strategy:
        strategy = _infer_strategy(config)

    logger.info("Invoke strategy selected: '%s'", strategy)
    if strategy == 'serverless':
        invoker = _invoke_serverless
    elif strategy == 'boto3':
        invoker = _invoke_boto3
    else:
        raise NotImplementedError("Could not find a way to invoke via '%s' strategy" % strategy)

    return invoker(config)

def _infer_strategy(config):
    if config['provider']['name'] == 'aws':
        if lambda_client:
            return 'boto3'
        else:
            logger.warning("Extras 'boto3' not installed. Falling back to 'serverless' invoke strategy")
    return 'serverless'


def _invoke_serverless(config, local=False):
    name = _get_lambda_name(config)
    command = 'serverless invoke'
    if local:
        command += ' local'

    command += ' --verbose --color --function %s' % name
    for line, retcode in run(command):
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
