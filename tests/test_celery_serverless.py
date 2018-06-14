#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `celery_worker_serverless` package."""

import time
import logging
import pytest
from pytest_shutil import env
from concurrent.futures import ThreadPoolExecutor

from click.testing import CliRunner

import celery_serverless
from celery_serverless import cli
from celery_serverless import handler_worker, handler_watchdog

logger = logging.getLogger(__name__)


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    # assert result.exit_code == 0
    # assert 'celery_serverless.cli.main' in result.output
    # help_result = runner.invoke(cli.main, ['--help'])
    # assert help_result.exit_code == 0
    # assert '--help  Show this message and exit.' in help_result.output


def test_worker_handler_minimal_call():
    response = handler_worker(None, None)
    assert response


def test_watchdog_handler_minimal_call():
    with env.set_env(CELERY_SERVERLESS_LOCK_URL='disabled', CELERY_SERVERLESS_QUEUE_URL='disabled'):
        response = handler_watchdog(None, None)
    assert response


_needed_parameters = ['CELERY_SERVERLESS_LOCK_URL', 'CELERY_SERVERLESS_QUEUE_URL']
@pytest.mark.parametrize('envname', _needed_parameters)
def test_watchdog_needs_envvar(envname):
    try:
        with env.unset_env([envname]):
            handler_watchdog(None, None)
    except AssertionError as err:
        assert 'envvar should be set' in str(err)
    else:
        raise RuntimeError('Had not raised an AssertionError')

@pytest.mark.timeout(30)
def test_watchdog_monitor_redis_queues(monkeypatch):
    queue_url = 'redis://'
    queue_name = 'celery'

    redis = pytest.importorskip('redis')
    from redis.exceptions import ConnectionError

    conn = redis.StrictRedis.from_url(queue_url)
    try:
        conn.ping()
    except ConnectionError as err:
        pytest.skip('Redis server is not available: %s' % err)

    # Redis is available.
    # Lets set it up before test the watchdog

    def _simulate_worker_invocation(*args, **kwargs):
        """
        Simulates a Worker invocation cycle via Redis keys changes
        """
        logger.warning('Simulating an Worker invocation: START')

        # Worker takes some time to init, then notify the Cache
        time.sleep(2)
        conn.incr('celery_serverless:watchdog:workers_started')

        # Worker gets a job to do, then notify the cache
        time.sleep(2)
        conn.rpop(queue_name)  # Gotten jobs drops from the queue.
        conn.incr('celery_serverless:watchdog:workers_fulfilled')

        # Note that we do not care where it finished. Just that started
        # AND got dropped from the queue
        logger.warning('Simulating an Worker invocation: END')

    _simulate_worker_invocation()   # Just be sure that it works.

    jobs = ['one', 'two', 'three']
    with conn.pipeline() as pipe:
        pipe.delete(queue_name)
        pipe.lpush(queue_name, *jobs)
        pipe.llen(queue_name)
        pipe_result = pipe.execute()

    assert pipe_result[-1] == 3, 'Are our Redis misbehaving or something?'

    with ThreadPoolExecutor() as executor:
        monkeypatch.setattr(
            'celery_serverless.watchdog.invoke_worker',
            lambda: (True, executor.submit(_simulate_worker_invocation)),
        )

        with env.set_env(CELERY_SERVERLESS_QUEUE_URL=queue_url, CELERY_SERVERLESS_LOCK_URL=queue_url):
            response = handler_watchdog(None, None)

    assert response

    assert conn.llen(queue_name) == 0, 'Watchdog finished but the queue is not empty'
    assert int(conn.get('celery_serverless:watchdog:workers_fulfilled')) >= len(jobs)
