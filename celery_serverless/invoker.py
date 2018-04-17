# coding: utf-8
from __future__ import unicode_literals, absolute_import

import functools
import logging
import codecs
import json
from pprint import pformat

import dirtyjson
import click
from celery_serverless.config import get_config


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

try:
    import boto3
    import botocore
    try:
        lambda_client = boto3.client('lambda')
    except botocore.exceptions.NoRegionError:
        logger.warning("'boto3' invoker cannot be used: please set a default AWS region on serverless.yml")
        lambda_client = None
except ImportError:  # Boto3 is an optional extra on setup.py
    lambda_client = None

from .cli_utils import run


CELERY_HANDLER_PATH = 'celery_serverless.handler_worker'


class Invoker(object):
    def __init__(self, config=None):
        self.config = config or get_config()

    def invoke_main(self, strategy=''):
        if not strategy:
            strategy = self._infer_strategy()

        logger.info("Invoke strategy selected: '%s'", strategy)
        if strategy == 'serverless':
            invoker = self._invoke_serverless
        elif strategy == 'boto3':
            invoker = self._invoke_boto3
        else:
            raise NotImplementedError("Could not find a way to invoke via '%s' strategy" % strategy)

        try:
            logs = invoker()  # Should raise exception on some problem
        except RuntimeError as err:
            logger.warning('Invocation failed via "%s": %s', strategy, err.details)
        return True


    def _infer_strategy(self):
        if self.config['provider']['name'] == 'aws':
            if lambda_client:
                return 'boto3'
            else:
                logger.warning("Invoke strategy 'boto3' could not be used. Falling back to 'serverless'")
        return 'serverless'


    def _invoke_serverless(self, local=False):
        name = _get_serverless_name(self.config)
        command = 'serverless invoke'
        if local:
            command += ' local'

        logger.debug("Invoking via 'serverless'")
        command += ' --log --verbose --function %s' % name

        output, retcode = next(run(command, out='oneshot'))

        logger.debug("Invocation logs from 'serverless':\n%s", output)

        if retcode != 0:
            error = RuntimeError('Invocation failed on Serverless: %s' % command)

            try:
                details = dirtyjson.loads(output)
            except ValueError:  # No JSON in the output
                details = {}

            if isinstance(details, dict):
                details = dict(details)
            elif isinstance(details, list):
                details = list(details)

            error.logs = output
            error.details = details
            raise error
        return output


    def _invoke_boto3(self):
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

        output = codecs.decode(response['LogResult'].encode(), 'base64')

        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug("Invocation logs from 'boto3':\n%s", output)

        if 'FunctionError' in response:
            error = RuntimeError('Invocation failed on AWS Lambda: %s' % lambda_arn)
            error.logs = output
            error.details = json.load(response['Payload'])
            raise error

        return output


def invoke(config=None, *args, **kwargs):
    return Invoker(config=config).invoke_main(*args, **kwargs)


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
