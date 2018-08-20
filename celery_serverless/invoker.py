# coding: utf-8
from __future__ import unicode_literals, absolute_import

import os
import functools
import logging
import codecs
import json
import multiprocessing
from pprint import pformat

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
from .utils import run_aio_on_thread, get_client_lock


CELERY_HANDLER_PATHS = {
    'worker': 'celery_serverless.handler_worker',
    'watchdog': 'celery_serverless.handler_watchdog',
}


def _get_serverless_name(config, target):
    for name, options in config['functions'].items():
        if options.get('handler') == CELERY_HANDLER_PATHS[target]:
            return name

    raise RuntimeError((
        "Handler '%s' not found on serverless.yml.\n"
        "Please fix it or run 'celery serverless init' to recreate one"
    ) % CELERY_HANDLER_PATHS[target])


@functools.lru_cache(8)
def _get_awslambda_arn(function_name):
    def _functions():
        for page in lambda_client.get_paginator('list_functions').paginate():
            for f in page.get('Functions', []):
                yield f

    for func in _functions():
        if func['FunctionName'] == function_name:
            return func['FunctionArn']

    raise RuntimeError('Handler %s not found deployed on service %s', handler_name, filter_string)


class Invoker(object):
    def __init__(self, target='worker', config=None):
        self.target = target
        self.config = config or get_config()

    def invoke_main(self, strategy='', stage='', extra_data=None):
        extra_data = extra_data or {}

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
            logs, future = invoker(stage=stage, extra_data=extra_data)  # Should raise exception on some problem
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
        name = _get_serverless_name(self.config, self.target)
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
                details = json.loads(output)
            except Exception:  # No JSON in the output
                details = output

            if isinstance(details, dict):
                details = dict(details)
            elif isinstance(details, list):
                details = list(details)

            error.logs = output
            error.details = details
            raise error
        return output, None

    def _invoke_boto3(self, stage='', sync=False, executor='asyncio', extra_data=None):
        extra_data = extra_data or {}
        stage = stage or self._get_stage()
        function_name = '%s-%s-%s' % (self.config['service'], stage,
                                      _get_serverless_name(self.config, self.target))
        lambda_arn = _get_awslambda_arn(function_name)
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

        if extra_data:
            invoke_options.update(dict(
                Payload=json.dumps(extra_data).encode()
            ))

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


def invoke(target='watchdog', config=None, *args, **kwargs):
    return Invoker(target=target, config=config).invoke_main(*args, **kwargs)


def invoke_worker(config=None, data=None, *args, **kwargs):
    return invoke(target='worker', extra_data=data or {}, *args, **kwargs)


def invoke_watchdog(config=None, data=None, check_lock=True, *args, **kwargs):
    from .watchdog import get_watchdog_lock

    if check_lock:
        lock_watchdog = get_watchdog_lock(enforce=False)[0]
        if lock_watchdog.locked():
            logger.info('Watchdog lock already held. Giving up.')
            return False, RuntimeError('Watchdog lock already held')
    return invoke(target='watchdog', extra_data=data or {}, *args, **kwargs)


def client_invoke_watchdog(check_lock=True, blocking_lock=False, *args, **kwargs):
    if not check_lock:
        logger.debug('Not checking client lock')
        return invoke_watchdog(check_lock=True, *args, **kwargs)

    client_lock = get_client_lock()[0]
    locked = client_lock.acquire(blocking_lock)
    if not locked:
        logger.info('Could not get Client lock. Giving up.')
        return False, RuntimeError('Client lock already held')

    logger.debug('Got the client lock')
    try:
        return invoke_watchdog(with_lock=True, *args, **kwargs)
    finally:
        logger.debug('Releasing the client lock')
        client_lock.release()
