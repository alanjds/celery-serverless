#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `celery_worker_serverless` package."""

import time
import uuid
import logging
import pytest
from pytest_shutil import env
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from celery_serverless import watchdog, invoker

logger = logging.getLogger(__name__)


@pytest.mark.timeout(30)
def test_watchdog_monitor_redis_queues(monkeypatch):
    lock_url = 'redis://'

    redis = pytest.importorskip('redis')
    from redis.exceptions import ConnectionError

    conn = redis.StrictRedis.from_url(lock_url)
    try:
        conn.ping()
    except ConnectionError as err:
        pytest.skip('Redis server is not available: %s' % err)

    # Redis is available.
    # Lets set it up before test the watchdog

    times_invoked = 0

    def _simulate_watchdog_invocation(*args, **kwargs):
        """
        Simulates a Watchdog invocation cycle via Redis locks changes
        """
        logger.warning('Simulating an Watchdog invocation: START')

        nonlocal times_invoked
        times_invoked += 1

        # 1) Watchdog fetches its lock
        lock, lock_name = watchdog.get_watchdog_lock(enforce=True)

        # 2) It runs with the lock or cancels
        with_lock = lock.acquire(False)
        if not with_lock:
            logger.info('Watchdog COULD NOT got the lock')
        else:
            logger.info('Watchdog GOT the lock')
            time.sleep(5)
            lock.release()
            logger.info('Watchdog RELEASED the lock')

        logger.warning('Simulating an Watchdog invocation: END')

    conn.flushdb()
    with env.set_env(CELERY_SERVERLESS_LOCK_URL=lock_url):
        _simulate_watchdog_invocation()   # Just be sure that it works.

    with ThreadPoolExecutor() as executor:
        monkeypatch.setattr(
            'celery_serverless.invoker.invoke',
            lambda *ar, **kw: (True, executor.submit(_simulate_watchdog_invocation)),
        )

        client_futures = []
        times_invoked = 0

        with env.set_env(CELERY_SERVERLESS_LOCK_URL=lock_url):
            for i in range(20):
                client_futures.append(executor.submit(invoker.client_invoke_watchdog))

            client_invokes = [fut.result() for fut in as_completed(client_futures)]
            result_flags, results = zip(*client_invokes)

        assert times_invoked == 1, 'More than one client succeeded to invoke_watchdog'
        assert len([i for i in result_flags if i == True]) == 1, 'More than one client got a lock'
