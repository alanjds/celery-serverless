# coding: utf-8
import os
import time
import operator
import logging
import threading
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from datetime import datetime, timezone, timedelta

import backoff
from redis import StrictRedis
from kombu import Connection
from kombu.transport import pyamqp
from celery_serverless.invoker import invoke

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

invoke_worker = partial(invoke, target='worker')

DEFAULT_BASENAME = 'celery_serverless:watchdog'
DEFAULT_BUCKET_EXPIRE = 6 * 60  # 6 minutes


class Watchdog(object):
    def __init__(self, communicator=None, name='', lock=None, watched=None):
        self._intercom = communicator or StrippedLocMemCache()
        self._name = name or DEFAULT_BASENAME
        self._lock = lock or threading.Lock()
        self._watched = watched

        # 0) Clear counters
        self.workers_started = 0
        self.workers_fulfilled = 0
        self.executor = ThreadPoolExecutor()

    #
    # Number we need. Calculated from the _cache
    #

    @property
    def workers_started(self):
        return int(self._cache.get('%s:%s' % (self._name, 'workers_started')) or 0)

    @workers_started.setter
    def workers_started(self, value):
        self._cache.set('%s:%s' % (self._name, 'workers_started'), value)

    @property
    def workers_fulfilled(self):
        return int(self._cache.get('%s:%s' % (self._name, 'workers_fulfilled')) or 0)

    @workers_fulfilled.setter
    def workers_fulfilled(self, value):
        self._cache.set('%s:%s' % (self._name, 'workers_fulfilled'), value)

    #
    # Numbers calculated from the ones we do have:
    #

    @property
    def workers_not_served(self):
        unfulfilled = self.workers_started - self.workers_fulfilled
        return unfulfilled if unfulfilled > 0 else 0

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

    def trigger_workers(self, how_much:int):
        logger.info('Starting %s workers', how_much)
        # Hack to call parameterless 'invoke_worker' func -> lambda x: invoke_worker
        return len([i for i in self.executor.map(lambda x: invoke_worker(), range(how_much))])

    @backoff.on_predicate(backoff.fibo, max_value=10)  # Will backoff until return True-ly val
    def _wait_start_notifications(self, starts:int):
        started = self.workers_started
        logger.debug('Started so far: %s', started)
        return (started >= starts)

    @backoff.on_predicate(backoff.fibo, predicate=operator.truth, max_value=9, max_time=30)
    def _wait_fulfillment(self):    # Stop backoff when 0 not_served or on max_time reached
        not_served = self.workers_not_served
        logger.info('Workers still unserved: %s', not_served)
        return not_served

    def monitor(self):
        for loops in count(1):  # while True
            logger.debug('Monitor loop started! [%s]', loops)

            # 1) See queue length N; 2) Start N workers
            started = self.trigger_workers(self.get_queue_length())
            if not started:
                break

            # 3) Watch for N starts
            self._wait_start_notifications(started)

            # 4) Wait then collect "Not Served" number
            unserved = self._wait_fulfillment()
            if unserved:
                logger.warning('Unserved %s workers', unserved)

        return self.workers_started  # How many had to be started to fulfill the queue?


class StrippedLocMemCache(object):
    # Stripped from Django's LocMemCache.
    # See: https://github.com/django/django/blob/master/django/core/cache/backends/locmem.py
    def __init__(self):
        self._cache = {}

    def get(self, key, default=None):
        return self._cache.get(key, default)

    def set(self, key, value):
        self._cache[key] = value

    def incr(self, key, delta=1):
        self._cache.setdefault(key, 0)
        self._cache[key] += delta
        return self.get(key)


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

    def __len__(self):
        if self._maybe_dirty:
            time.sleep(self.KOMBU_HEARTBEAT * 1.5)
        result = self.connection.channel()._size(self.queue)

        # Kombu queue length will not change until next heartbeat.
        # Would be better to use some token-bucket timeout,
        # but some `time.delay()` will do for now.
        self._maybe_dirty = True
        return result


def _cap_to_minute(now):
    return now.replace(second=0, microsecond=0)


def get_workers_all_key(prefix=DEFAULT_BASENAME):
    return '%s:workers:all' % prefix


def get_workers_bucket_key(prefix=DEFAULT_BASENAME, now=None):
    this_minute = _cap_to_minute(now or datetime.now(timezone.utc))
    return '%s:workers:%s' % (prefix, this_minute.isoformat())


def inform_worker_join(redis:'StrictRedis', worker_id:str, bucket='', prefix=DEFAULT_BASENAME, now=None):
    bucket = bucket or get_workers_bucket_key(prefix=prefix, now=now)
    with redis.pipeline() as pipe:
        pipe.sadd(bucket, worker_id)
        pipe.expire(bucket, DEFAULT_BUCKET_EXPIRE)
        pipe.publish(get_workers_all_key(prefix=prefix) + '[join]', worker_id)
        pipe.execute()
    return bucket


def inform_worker_working(redis:'StrictRedis', worker_id:str, prefix=DEFAULT_BASENAME):
    return redis.publish(get_workers_all_key(prefix=prefix) + '[working]', worker_id)


def inform_worker_leave(redis:'StrictRedis', worker_id:str, bucket:str):
    with redis.pipeline() as pipe:
        pipe.srem(bucket, worker_id)
        pipe.publish(get_workers_all_key() + '[leave]', worker_id)
        was_removed, _ = pipe.execute()
    return was_removed


def refresh_workers_all_key(redis:'StrictRedis', prefix=DEFAULT_BASENAME, now=None, minutes=5):
    workers_all_key = get_workers_all_key(prefix=prefix)

    this_minute = _cap_to_minute(now or datetime.now(timezone.utc))
    worker_buckets = []
    for i in range(minutes):
        target_time = this_minute - timedelta(minutes=i)
        worker_buckets.append(get_workers_bucket_key(prefix=prefix, now=target_time))

    with redis.pipeline() as pipe:
        pipe.sunionstore(workers_all_key, worker_buckets)
        pipe.expire(workers_all_key, DEFAULT_BUCKET_EXPIRE)
        workers_len, _ = pipe.execute()

    return workers_len, workers_all_key, worker_buckets
