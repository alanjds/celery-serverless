# coding: utf-8
from __future__ import unicode_literals, absolute_import

import functools
import logging
import codecs
import json
from pprint import pformat
from io import BytesIO

import dirtyjson
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

    try:
        logs = invoker(config)  # Should raise exception on some problem
    except RuntimeError as err:
        if logger.getEffectiveLevel() <= logging.INFO:
            logger.info('Invocation failed via "%s": %s', strategy, err.details)
        raise
    return True


def _infer_strategy(config):
    if config['provider']['name'] == 'aws':
        if lambda_client:
            return 'boto3'
        else:
            logger.warning("Invoke strategy 'boto3' could not be used. Falling back to 'serverless'")
    return 'serverless'


def _invoke_serverless(config, local=False):
    name = _get_serverless_name(config)
    command = 'serverless invoke'
    if local:
        command += ' local'

    logger.debug("Invoking via 'serverless'")
    command += ' --log --verbose --function %s' % name
    output = BytesIO()
    for line, retcode in run(command, output):
        click.echo(line, nl=False)

    output.seek(0)
    if retcode != 0:
        error = RuntimeError('Command failed: %s' % command)

        details = dirtyjson.loads(output.read().decode())
        if isinstance(details, dict):
            details = dict(details)
        elif isinstance(details, list):
            retails = list(details)

        error.logs = output
        error.details = details
        raise error
    return output


def _invoke_boto3(config):
    lambda_arn = _get_awslambda_arn(CELERY_HANDLER_PATH)
    logger.debug("Invoking via 'boto3'")
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType='RequestResponse', # 'RequestResponse'|'Event'|'DryRun'
        LogType='Tail',  # 'None'|'Tail'
        #ClientContext='string',
        #Payload=b'bytes'|file,
        #Qualifier='$LATEST',  # 'string'
    )

    if logger.getEffectiveLevel() <= logging.DEBUG:
        log_output = pformat(response)
        logger.debug("Invocation response from 'boto3':\n%s", response)

    output = BytesIO(codecs.decode(response['LogResult'].encode(), 'base64'))

    if logger.getEffectiveLevel() <= logging.INFO:
        output.seek(0)
        logger.info("Invocation logs from 'boto3':\n%s", output.read().decode())

    if 'FunctionError' in response:
        output.seek(0)
        error = RuntimeError('Invocation failed on AWS Lambda: %s' % lambda_arn)
        error.logs = output
        error.details = json.load(response['Payload'])
        raise error

    output.seek(0)
    return output


def _get_serverless_name(config):
    for name, options in config['functions'].items():
        if options.get('handler') == CELERY_HANDLER_PATH:
            return name

    raise RuntimeError((
        "Handler '%s' not found on serverless.yml.\n"
        "Please fix it or run 'celery serverless init' to recreate one"
    ) % CELERY_HANDLER_PATH)


@functools.lru_cache(8)
def _get_awslambda_arn(lambda_name):
    for func in lambda_client.list_functions().get('Functions', []):
        if func['Handler'] == lambda_name:
            return func['FunctionArn']
