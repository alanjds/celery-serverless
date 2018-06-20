# coding: utf-8
import time
import uuid
import logging
import threading
from functools import partial
from concurrent.futures import as_completed
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
UNCONFIRMED_LIMIT = {'seconds': 30}


class Watchdog(object):
    def __init__(self, communicator=None, name='', lock=None, watched=None):
        self._intercom = communicator
        self._name = name or DEFAULT_BASENAME
        self._lock = lock or threading.Lock()
        self._watched = watched
        self.pool_size = 200

        # 0) Clear counters
        self.joined_event_count = 0

    def get_workers_count(self):
        raise NotImplementedError()
        if hasattr(self._intercom, 'get_workers_count'):
            return self._intercom.get_workers_count()
        return refresh_workers_all_key(self._intercom)[0]

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

    def _inform_new_worker(self, worker_id: str):
        """
        Inform the central state in self._intercom that a new worker joined.
        Sets the expiration of the state.
        """
        raise NotImplementedError('Create a new redis key and set the expiration')
        if isinstance(self._intercom, MuteIntercom):
            return None

        with self._intercom.pipeline() as pipe:
            pipe.sadd(bucket, worker_id)
            pipe.expire(bucket, DEFAULT_BUCKET_EXPIRE)
            pipe.execute()
        return key

    def trigger_worker(self):
        """
        Generates a new worker id, adds a REDIS key with this id and the current timestamp
        and invokes the worker.
        :return: A tuple: (Triggered worker ID,) + invoke_worker()
        """
        worker_uuid = uuid.uuid1()
        self._inform_new_worker(worker_uuid)
        return invoke_worker(worker_id=worker_uuid, worker_trigger_time=datetime.now()) + (worker_uuid,)

    def trigger_workers(self, how_many:int):
        if not how_many:
            return 0
        logger.info('Starting %s workers', how_many)
        success_calls = 0
        invocations = []
        for i in range(how_many):
            triggered, future, worker_id = self.trigger_worker()
            invocations.append(future)

        for future in as_completed(invocations):
            try:
                future.result()
                success_calls += 1
            except Exception as err:
                logger.error('Invocation failed', exc_info=1)

        return success_calls

    def monitor(self):
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

            self.trigger_workers(available_workers)

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


def inform_worker_leave(redis:'StrictRedis', worker_id:str):
    raise NotImplementedError()
    if isinstance(redis, MuteIntercom):
        return None

    with redis.pipeline() as pipe:
        pipe.srem((get_workers_all_key(), worker_id))
        was_removed, _ = pipe.execute()
    return was_removed


def refresh_workers_all_key(redis:'StrictRedis', prefix=DEFAULT_BASENAME, now=None, minutes=5):
    raise NotImplementedError()

    def get_workers_all_key(prefix=DEFAULT_BASENAME):
        return '%s:workers:all' % prefix

    def get_workers_bucket_key(prefix=DEFAULT_BASENAME, now=None):
        return '%s:workers:%s' % (prefix, this_minute.isoformat())

    if isinstance(redis, MuteIntercom):
        return None, None, None

    workers_all_key = get_workers_all_key(prefix=prefix)

    this_minute = _truncate_to_minute(now or datetime.now(timezone.utc))
    worker_buckets = []
    for i in range(minutes):
        target_time = this_minute - timedelta(minutes=i)
        worker_buckets.append(get_workers_bucket_key(prefix=prefix, now=target_time))

    with redis.pipeline() as pipe:
        pipe.sunionstore(workers_all_key, worker_buckets)
        pipe.expire(workers_all_key, DEFAULT_BUCKET_EXPIRE)
        workers_len, _ = pipe.execute()

    return workers_len, workers_all_key, worker_buckets
