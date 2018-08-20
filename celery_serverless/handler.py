# coding: utf-8
try:  # https://github.com/UnitedIncome/serverless-python-requirements#dealing-with-lambdas-size-limitations
    import unzip_requirements
except ImportError:
    pass

import os
import time
import json
import logging

from celery_serverless.handler_utils import handler_wrapper

logger = logging.getLogger(__name__)
logger.propagate = True
if os.environ.get('CELERY_SERVERLESS_LOGLEVEL'):
    logger.setLevel(os.environ.get('CELERY_SERVERLESS_LOGLEVEL'))
print('Celery serverless loglevel:', logger.getEffectiveLevel())

from redis import StrictRedis
from redis.lock import LockError
from timeoutcontext import timeout as timeout_context
from celery_serverless.invoker import invoke_watchdog
from celery_serverless.watchdog import Watchdog, KombuQueueLengther, build_intercom, get_watchdog_lock
from celery_serverless.worker_management import WorkerRunner, remaining_lifetime_getter

_worker_runner = WorkerRunner()


@handler_wrapper
def worker(event, context, intercom_url=None):
    event = event or {}

    logger.debug('Event: %s', event)

    _worker_runner.lifetime_getter = remaining_lifetime_getter(context)
    _worker_runner.set_worker_metadata(intercom_url=intercom_url, worker_metadata=event or {})

    if not _worker_runner.hooks:
        logger.debug('Fresh Celery worker. Attach hooks!')
        _worker_runner.attach_hooks()
    else:
        logger.debug('Old Celery worker. Already have hooks.')

    # The Celery worker will start here. Will connect, take 1 (one) task,
    # process it, and quit.
    logger.debug('Spawning the worker(s)')
    _worker_runner.run_worker(loglevel='DEBUG')  # Will block until at least one task got processed

    logger.debug('Cleaning up before exit')
    body = {
        "message": "Celery worker worked, lived, and died.",
    }
    return {"statusCode": 200, "body": json.dumps(body)}


@handler_wrapper
def watchdog(event, context):
    queue_url = os.environ.get('CELERY_SERVERLESS_QUEUE_URL')
    assert queue_url, 'The CELERY_SERVERLESS_QUEUE_URL envvar should be set. Even to "disabled" to disable it.'

    intercom_url = os.environ.get('CELERY_SERVERLESS_INTERCOM_URL')
    assert intercom_url, 'The CELERY_SERVERLESS_INTERCOM_URL envvar should be set. Even to "disabled" to disable it.'

    lock, lock_name = get_watchdog_lock(enforce=True)

    if queue_url == 'disabled':
        watched = None
    else:
        watched = KombuQueueLengther(queue_url, 'celery')   # TODO: Allow queue name to be chosen

    watchdog = Watchdog(communicator=build_intercom(intercom_url), name=lock_name, lock=lock, watched=watched)

    try:
        remaining_seconds = context.get_remaining_time_in_millis() / 1000.0
    except Exception as e:
        logger.exception('Could not got remaining_seconds. Is the context right?')
        remaining_seconds = 5 * 60 # 5 minutes by default

    spare_time = 30  # Should be enough time to retrigger this FaaS again
    fulfilled = False
    try:
        with timeout_context(remaining_seconds-spare_time):
            watchdog.monitor()
            fulfilled = True
    except TimeoutError:
        logger.warning('Still stuff to monitor but this Function is out of time. Preparing to reinvoke the Watchdog')

        # Be sure that the lock is released. Then reinvoke.
        try:
            lock.release()
        except (RuntimeError, AttributeError, LockError):
            pass

        logger.info('All set. Reinvoking the Watchdog')
        _, future = invoke_watchdog(check_lock=False)
        future.result()
        logger.info('Done reinvoking another Watchdog')

    logger.debug('Cleaning up before exit')
    body = {
        "message": "Watchdog woke, worked, and rested.",
        "fulfilled": fulfilled,
    }
    return {"statusCode": 200 if fulfilled else 202, "body": json.dumps(body)}
