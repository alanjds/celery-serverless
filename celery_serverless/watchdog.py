# coding: utf-8
import os
import time
import uuid
import logging
import dummy_threading
from functools import partial
from itertools import count
from concurrent.futures import TimeoutError as FuturesTimeoutError
from asyncio import Future as AsyncioFuture, InvalidStateError
from datetime import datetime, timezone, timedelta

import backoff
from redis import StrictRedis
from kombu import Connection
from kombu.transport import pyamqp
from celery_serverless.invoker import invoke_worker

from .utils import get_watchdog_lock

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

DEFAULT_BASENAME = 'celery_serverless:watchdog'
DEFAULT_WORKER_EXPIRE = 6 * 60  # 6 minutes
DEFAULT_STARTED_TIMEOUT = 30 # half minute


class Watchdog(object):
    def __init__(self, communicator=None, name='', lock=None, watched=None):
        self._intercom = communicator
        self._name = name or DEFAULT_BASENAME
        self._lock = lock or dummy_threading.Lock()
        self._watched = watched
        self.pool_size = 200

        # 0) Clear counters
        self.joined_event_count = 0

    def get_workers_count(self):
        if hasattr(self._intercom, 'get_workers_count'):
            return self._intercom.get_workers_count()
        return _get_workers_count(self._intercom)

    def get_workers_starting(self):
        if hasattr(self._intercom, 'get_workers_starting'):
            return self._intercom.get_workers_starting()
        return _get_workers_count(self._intercom, started=True, busy=False)

    def get_queue_length(self):
        if self._watched is None:
            logger.warning('Watchdog is watching None as queue. Fix it!')
            return 0
        len_ = len(self._watched)
        logger.debug('_watched reported %s jobs awaiting', len_)
        return len_

    #
    # Actions:
    #

    def _inform_worker_new(self, worker_id:str):
        """
        Inform the central state in self._intercom that a new worker joined.
        Sets the expiration of the state.
        """
        return inform_worker_new(self._intercom, worker_id, prefix=self._name)

    def _trigger_worker(self) -> tuple:
        """
        Generates a new worker id, adds a REDIS key with this id and the current
        timestamp and invokes the worker.

        :return: invoke_worker() + (new worker worker_id,)
        """
        worker_uuid = str(uuid.uuid1())
        _, worker_data = self._inform_worker_new(worker_uuid)
        success, future = invoke_worker(data={
            'worker_id': worker_data['id'],
            'worker_trigger_time': worker_data['time_join'],
            'prefix': self._name,
        })
        return success, future, worker_data

    def trigger_workers(self, how_many:int):
        if not how_many:
            return 0
        logger.info('Starting %s workers', how_many)

        success_calls = 0
        invocations = []
        for i in range(how_many):
            triggered, future, worker_data = self._trigger_worker()
            if not triggered:
                continue

            success_calls += 1
            invocations.append(future)

            def _done_callback(fut):
                try:
                    fut.result()
                except Exception as err:
                    logger.error('Could not trigger worker: [%s] %s', type(err), err, exc_info=True)
            future.add_done_callback(_done_callback)

        return success_calls

    def monitor(self):
        locked = self._lock.acquire(False)
        if not locked:
            logger.info('Could not get the lock. Giving up.')
            return 0

        try:
            for loops in count(1):  # while True
                logger.debug('Monitor loop started! [%s]', loops)

                # 1) See queue length N
                queue_length = self.get_queue_length()

                # 2a) Stop if empty queue and no running worker left
                existing_workers = self.get_workers_count()
                if not queue_length and not existing_workers:
                    logger.debug('Empty queue and no worker running. Stop monitoring')
                    break

                logger.debug('We have %s enqueued tasks and %s workers running', queue_length, existing_workers)

                # 2b) Start (N-existing) workers
                available_workers = self.pool_size - existing_workers
                available_workers = max(available_workers, 0)

                needed_workers = queue_length - self.get_workers_starting()
                desired_new_workers = min(needed_workers, available_workers)

                if desired_new_workers > 0:
                    success_calls = self.trigger_workers(desired_new_workers)
                    logger.info('Invoked %s of %s tried', desired_new_workers, success_calls)
        finally:
            self._lock.release()

        return self.joined_event_count  # How many had to be started to fulfill the queue?


class MuteIntercom(object):
    def get_workers_count(self):
        return 0


# Queue length with ideas from ryanhiebert/hirefire
# See: https://github.com/ryanhiebert/hirefire/blob/67d57c8/hirefire/procs/celery.py#L239
def _AMQPChannel_size(self, queue):
    try:
        from librabbitmq import ChannelError
    except ImportError:
        from amqp.exceptions import ChannelError

    try:
        queue = self.queue_declare(queue, passive=True)
    except ChannelError:
        # The requested queue has not been created yet
        count = 0
    else:
        count = queue.message_count

    return count
pyamqp.Channel._size = _AMQPChannel_size


class KombuQueueLengther(object):
    KOMBU_HEARTBEAT = 2

    def __init__(self, url, queue):
        self.connection = Connection(url, heartbeat=self.KOMBU_HEARTBEAT)
        self.queue = queue
        self._maybe_dirty = False

    @backoff.on_exception(backoff.fibo, ConnectionError, max_value=9, max_time=30, jitter=backoff.full_jitter)
    def __len__(self):
        if self._maybe_dirty:
            time.sleep(self.KOMBU_HEARTBEAT * 1.5)
        result = self.connection.channel()._size(self.queue)

        # Kombu queue length will not change until next heartbeat.
        # Would be better to use a token-bucket timeout,
        # but some `time.delay()` will do for now.
        self._maybe_dirty = True
        return result


def build_intercom(intercom):
    if not intercom or intercom == 'disabled':
        return MuteIntercom()
    elif isinstance(intercom, (bytes, str)):
        return StrictRedis.from_url(intercom)
    else:
        raise NotImplementedError()


def inform_worker_new(redis:'StrictRedis', worker_id:str, prefix=DEFAULT_BASENAME):
    """
    Inform the central state in self._intercom that a new worker joined.
    Sets the expiration of the state.
    """
    if isinstance(redis, MuteIntercom):
        return None

    worker_prefix = _get_worker_key_prefix(prefix=prefix)
    worker_key = worker_prefix + str(worker_id)
    workers_started_key = _get_workers_started_key(prefix=prefix)

    metadata = {
        'id': worker_id,
        'key': worker_key,
        'time_join': datetime.now(timezone.utc).timestamp(),  # secs from epoch
    }

    with redis.pipeline() as pipe:
        pipe.hmset(worker_key, metadata)
        pipe.expire(worker_key, DEFAULT_WORKER_EXPIRE)

        pipe.zadd(workers_started_key, **{worker_key: metadata['time_join']})
        pipe.expire(workers_started_key, DEFAULT_WORKER_EXPIRE)  # Renew expire limit
        result, _, result_zadd, *_ = pipe.execute()

    logger.info('Informed [new]: %s', worker_key)
    logger.debug('ZADD %s: %s', workers_started_key, result_zadd)
    return (worker_key, metadata) if result else result


def inform_worker_busy(redis:'StrictRedis', worker_id:str, prefix=DEFAULT_BASENAME):
    if isinstance(redis, MuteIntercom):
        return None

    workers_started_key = _get_workers_started_key(prefix=prefix)
    workers_busy_key = _get_workers_busy_key(prefix=prefix)
    worker_prefix = _get_worker_key_prefix(prefix=prefix)
    worker_key = worker_prefix + str(worker_id)
    epoch_now = datetime.now(timezone.utc).timestamp()  # secs from epoch

    with redis.pipeline() as pipe:
        pipe.zadd(workers_busy_key, **{worker_key: epoch_now})
        pipe.zrem(workers_started_key, worker_key)

        # Renew expire limits
        pipe.expire(worker_key, DEFAULT_WORKER_EXPIRE)
        pipe.expire(workers_busy_key, DEFAULT_WORKER_EXPIRE)
        pipe.expire(workers_started_key, DEFAULT_WORKER_EXPIRE)
        result_add, result_rem, *_ = pipe.execute()

    logger.info('Informed [busy]: %s', worker_key)
    logger.debug('ZADD %s: %s', workers_busy_key, result_add)
    logger.debug('ZREM %s: %s', workers_started_key, result_rem)
    return result_add


def inform_worker_leave(redis:'StrictRedis', worker_id:str, prefix=DEFAULT_BASENAME):
    if isinstance(redis, MuteIntercom):
        return None

    workers_started_key = _get_workers_started_key(prefix=prefix)
    workers_busy_key = _get_workers_busy_key(prefix=prefix)
    worker_prefix = _get_worker_key_prefix(prefix=prefix)
    worker_key = worker_prefix + str(worker_id)

    with redis.pipeline() as pipe:
        pipe.delete(worker_key)  # TODO: Use "UNLINK" instead of "DEL"
        pipe.zrem(workers_started_key, worker_key)
        pipe.zrem(workers_busy_key, worker_key)
        _, *deleted = pipe.execute()

    logger.info('Informed [leave]: %s', worker_key)
    logger.debug('ZREM %s: %s', workers_started_key, deleted[0])
    logger.debug('ZREM %s: %s', workers_busy_key, deleted[1])
    return len(deleted)


def _get_worker_key_prefix(prefix=DEFAULT_BASENAME):
    return '%s:worker:' % prefix


def _get_workers_started_key(prefix=DEFAULT_BASENAME):
    return '%s:workers:started' % prefix


def _get_workers_busy_key(prefix=DEFAULT_BASENAME):
    return '%s:workers:busy' % prefix


def _get_workers_count(redis:'StrictRedis', prefix=DEFAULT_BASENAME, now=None,
                       started=True, started_duration=None,
                       busy=True, busy_duration=None):
    assert started or busy, 'What are you counting if not started nor busy ones?'
    started_duration = started_duration or {'seconds': 30}
    busy_duration = busy_duration or {'seconds': DEFAULT_WORKER_EXPIRE}

    if isinstance(redis, MuteIntercom):
        return None

    now = now or datetime.now(timezone.utc)

    workers_started_key = _get_workers_started_key(prefix=prefix)
    workers_busy_key = _get_workers_busy_key(prefix=prefix)

    with redis.pipeline() as pipe:
        if started:
            start = int((now - timedelta(**started_duration)).timestamp())
            end = float('+inf')  # To infinite and beyond
            pipe.zcount(workers_started_key, start, end)
        if busy:
            start = int((now - timedelta(**busy_duration)).timestamp())
            end = float('+inf')  # To infinite and beyond
            pipe.zcount(workers_busy_key, start, end)
        count = sum(pipe.execute())

    return count
