# coding: utf-8
from __future__ import unicode_literals, absolute_import

import os
import functools
import logging
import codecs
import json
from pprint import pformat

import dirtyjson
import click
from future_thread import Future as FutureThread

from celery_serverless.config import get_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

try:
    import boto3
    import botocore
    import aioboto3
    try:
        lambda_client = boto3.client('lambda')
    except botocore.exceptions.NoRegionError:
        logger.warning("'boto3' invoker cannot be used: please set a default AWS region on serverless.yml")
        lambda_client = None
except ImportError:  # Boto3 is an optional extra on setup.py
    lambda_client = None

from .cli_utils import run
from .utils import run_aio_on_thread

CELERY_HANDLER_PATH = 'celery_serverless.handler_worker'


class Invoker(object):
    def __init__(self, config=None):
        self.config = config or get_config()

    def invoke_main(self, strategy='', stage=''):
        if not strategy:
            strategy = self._infer_strategy()

        logger.info("Invoke strategy selected: '%s'", strategy)
        if strategy == 'serverless':
            invoker = self._invoke_serverless
        elif strategy == 'boto3':
            invoker = self._invoke_boto3
        else:
            raise NotImplementedError("Could not find a way to invoke via '%s' strategy" % strategy)

        if not stage:
            stage = self._get_stage()

        try:
            logs, future = invoker(stage=stage)  # Should raise exception on some problem
        except RuntimeError as err:
            logger.warning('Invocation failed via "%s": %s', strategy, getattr(err, 'details', ''))
            return False, err
        return True, future

    def _get_stage(self):
        stage = os.environ.get('CELERY_SERVERLESS_STAGE')
        if not stage:
            stage = self.config.get('stage')
        if not stage:
            stage = self.config['provider'].get('stage')
        if not stage:
            stage = 'dev'    # Default stage
        return stage

    def _infer_strategy(self):
        if self.config['provider']['name'] == 'aws':
            if lambda_client:
                return 'boto3'
            else:
                logger.warning("Invoke strategy 'boto3' could not be used. Falling back to 'serverless'")
        return 'serverless'

    def _invoke_serverless(self, stage='', local=False):
        name = _get_serverless_name(self.config)
        command = 'serverless invoke'
        if local:
            command += ' local'
        if stage:
            command += ' --stage %s' % stage

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
        return output, None

    def _invoke_boto3(self, stage='', sync=False, executor='asyncio'):
        stage = stage or self._get_stage()
        filter_string = '%s-%s-' % (self.config['service'], stage)
        lambda_arn = _get_awslambda_arn(CELERY_HANDLER_PATH, filter_string)
        assert lambda_arn, 'An exeception should had raised on _get_awslambda_arn call.'
        logger.debug("Invoking via 'boto3' %s %s", 'sync' if sync else 'async', executor)
        future = None
        output = ''

        invoke_options = dict(
            FunctionName=lambda_arn,
            #ClientContext='string',
            #Payload=b'bytes'|file,
            #Qualifier='$LATEST',  # 'string'
        )

        if sync:
            invoke_options.update(dict(
                InvocationType='RequestResponse', # 'RequestResponse'|'Event'|'DryRun'
                LogType='Tail',  # 'None'|'Tail'
            ))
        else:
            invoke_options.update(dict(
                InvocationType='Event'
            ))

        if executor == 'asyncio':
            logger.debug('Invoking via asyncio')
            async def _func():
                async with aioboto3.client('lambda') as cli:
                    res = await cli.invoke(**invoke_options)
                return res
            future = run_aio_on_thread(_func())

        elif executor == 'threading':
            logger.debug('Invoking via threading')
            future = FutureThread(lambda_client.invoke, **invoke_options)

        elif executor == 'linear':   # Fallback to the traditional "linear" way
            logger.debug('Invoking via "linear" inline code')
            response = lambda_client.invoke(**invoke_options)

            if logger.getEffectiveLevel() <= logging.DEBUG:
                log_output = pformat(response)
                logger.debug("Invocation response from 'boto3':\n%s", response)

            output = codecs.decode(response['LogResult'].encode(), 'base64') if sync else ''

            if output and logger.getEffectiveLevel() <= logging.DEBUG:
                logger.debug("Invocation logs from 'boto3':\n%s", output)

            if 'FunctionError' in response:
                error = RuntimeError('Invocation failed on AWS Lambda: %s' % lambda_arn)
                error.logs = output
                error.details = json.load(response['Payload'])
                raise error

        else:
            raise TypeError("'executor' not found")

        return output, future


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
def _get_awslambda_arn(handler_name, filter_string):
    def _functions():
        for page in lambda_client.get_paginator('list_functions').paginate():
            for f in page.get('Functions', []):
                yield f

    for func in _functions():
        if func['Handler'] == handler_name and filter_string in func['FunctionName']:
            return func['FunctionArn']

    raise RuntimeError('Handler %s not found deployed on service %s', handler_name, filter_string)
