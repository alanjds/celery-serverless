# coding: utf-8
import os
import time
import operator
import logging
import threading
from pprint import pformat
from functools import partial
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        self.workers_started = 0
        self._unconfirmed_registry = OrderedDict()
        self._unconfirmed_registry_lock = threading.Lock()
        self._pubsub = self._intercom.pubsub() if isinstance(self._intercom, StrictRedis) else None
        if self._pubsub:
            self._init_pubsub()

    def _init_pubsub(self):
        """
        Creates a bunch of callbacks for events sent by our workers.
        """
        channel_join_key = get_workers_all_key(prefix=self._name) + '[join]'
        def _handle_join_event(message):
            logging.debug('[event:join] Worker joined')
            self.workers_started += 1

        channel_working_key = get_workers_all_key(prefix=self._name) + '[working]'
        def _handle_working_event(message):
            logging.debug('[event:working] Worker got a job')
            self.confirm_worker()

        channel_leave_key = get_workers_all_key(prefix=self._name) + '[leave]'
        def _handle_leave_event(message):
            logging.debug('[event:leave] Worker rested')

        self._subscription_hooks = {    # Prevents GC. Should hold the handle.
            channel_join_key: _handle_join_event,
            channel_working_key: _handle_working_event,
            channel_leave_key: _handle_leave_event,
        }
        self._pubsub.subscribe(**self._subscription_hooks)
        self._pubsub.run_in_thread(daemon=True)

    def register_unconfirmed_workers(self, how_many=1):
        assert how_many >= 0, 'do _confirm_worker instead of register negatives'
        now = datetime.now().replace(microsecond=0)  # Capped to seconds buckets
        self._unconfirmed_registry.setdefault(now, 0)
        self._unconfirmed_registry[now] += how_many

    def confirm_worker(self):
        with self._unconfirmed_registry_lock:
            now = datetime.now()

            for time, count in self._unconfirmed_registry.items():
                if time + timedelta(**UNCONFIRMED_LIMIT) < now:
                    # This register is expired.
                    self._unconfirmed_registry.pop(time)
                    continue

                # Found!
                break
            else:
                # No register found or all already expired
                return False

            # Consume the found stuff.
            if count > 1:
                self._unconfirmed_registry[time] -= 1
            else:  # 1 or less
                self._unconfirmed_registry.pop(time)

            return True

    def get_workers_unconfirmed(self):
        logger.debug('_unconfirmed_registry: %s', pformat(dict(self._unconfirmed_registry)))

        now = datetime.now()
        valid = 0
        invalid = []
        for time, count in self._unconfirmed_registry.items():
            if time + timedelta(**UNCONFIRMED_LIMIT) >= now:
                # Count up the valid times key values
                valid += count
            else:
                invalid.append(time)

        # Clear the expired times keys
        for k in invalid:
            self._unconfirmed_registry.pop(k)
        return valid

    def get_workers_count(self):
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

    def trigger_workers(self, how_much:int):
        logger.info('Starting %s workers', how_much)
        success_calls = 0
        invocations = []
        for i in range(how_much):
            triggered, future = invoke_worker()
            if triggered:
                self.register_unconfirmed_workers(1)
            invocations.append(future)

        for future in as_completed(invocations):
            try:
                future.result()
                success_calls += 1
            except Exception as err:
                self.confirm_worker()  # Confirmed as failed
                logger.error('Invocation failed', exc_info=1)

        return success_calls

    def monitor(self):
        for loops in count(1):  # while True
            logger.debug('Monitor loop started! [%s]', loops)

            # 1) See queue length N
            queue_length = self.get_queue_length()

            # 2a) Stop if empty queue and no running worker left
            existing_workers = self.get_workers_count()
            if not queue_length:
                if existing_workers:
                    logger.debug('Empty queue, but still %s workers running', existing_workers)
                else:  # No queue and no workers: Stop monitoring
                    logger.debug('Empty queue and no worker running. Stop monitoring')
                    unconfirmed = self.get_workers_unconfirmed()
                    if unconfirmed:
                        logger.warning('Exiting with %s still unconfirmed workers!', unconfirmed)
                    break

            # 2b) Start (N-existing) workers
            unconfirmed = self.get_workers_unconfirmed()
            available_workers = self.pool_size - existing_workers - unconfirmed
            available_workers = max(available_workers, 0)
            unhandled_queued = queue_length - unconfirmed

            to_trigger = min(unhandled_queued, available_workers)
            if to_trigger > 0:
                self.trigger_workers(to_trigger)

            # 3b) Watch for N starts
            # started = self.wait_start_notifications(triggered)

            # 4) Watch for N working notifications
            # working = self.wait_working_notifications(started)

        return self.workers_started  # How many had to be started to fulfill the queue?


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


def _cap_to_minute(now):
    return now.replace(second=0, microsecond=0)


def get_workers_all_key(prefix=DEFAULT_BASENAME):
    return '%s:workers:all' % prefix


def get_workers_bucket_key(prefix=DEFAULT_BASENAME, now=None):
    this_minute = _cap_to_minute(now or datetime.now(timezone.utc))
    return '%s:workers:%s' % (prefix, this_minute.isoformat())


def build_intercom(intercom):
    if not intercom or intercom == 'disabled':
        return MuteIntercom()
    elif isinstance(intercom, (bytes, str)):
        return StrictRedis.from_url(intercom)
    else:
        raise NotImplementedError()


def inform_worker_join(redis:'StrictRedis', worker_id:str, bucket='', prefix=DEFAULT_BASENAME, now=None):
    if isinstance(redis, MuteIntercom):
        return None

    bucket = bucket or get_workers_bucket_key(prefix=prefix, now=now)
    with redis.pipeline() as pipe:
        pipe.sadd(bucket, worker_id)
        pipe.expire(bucket, DEFAULT_BUCKET_EXPIRE)
        pipe.publish(get_workers_all_key(prefix=prefix) + '[join]', worker_id)
        pipe.execute()
    return bucket


def inform_worker_working(redis:'StrictRedis', worker_id:str, prefix=DEFAULT_BASENAME):
    if isinstance(redis, MuteIntercom):
        return None
    return redis.publish(get_workers_all_key(prefix=prefix) + '[working]', worker_id)


def inform_worker_leave(redis:'StrictRedis', worker_id:str, bucket:str):
    if isinstance(redis, MuteIntercom):
        return None

    with redis.pipeline() as pipe:
        pipe.srem(bucket, worker_id)
        pipe.publish(get_workers_all_key() + '[leave]', worker_id)
        was_removed, _ = pipe.execute()
    return was_removed


def refresh_workers_all_key(redis:'StrictRedis', prefix=DEFAULT_BASENAME, now=None, minutes=5):
    if isinstance(redis, MuteIntercom):
        return None, None, None

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
