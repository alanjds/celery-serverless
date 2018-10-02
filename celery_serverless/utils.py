# coding: utf-8
import os
import asyncio
from threading import Thread
from inspect import isawaitable
import importlib

try:
    from redis import StrictRedis

    # MONKEYPATCH until https://github.com/andymccurdy/redis-py/pull/1007/ is merged
    import redis.lock
    def __locked(self):
        if self.redis.get(self.name) is not None:
            return True
        return False
    if not hasattr(redis.lock.Lock, 'locked'):
        redis.lock.Lock.locked = __locked
except ImportError:
    pass

slave_loop = None    # loop
slave_thread = None  # type: Thread


def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _start_thread():
    global slave_loop
    global slave_thread
    slave_loop = slave_loop or asyncio.new_event_loop()
    slave_thread = slave_thread or Thread(target=_start_loop, args=(slave_loop,))
    slave_thread.daemon = True   # Clean itself on Python exit.
    slave_thread.start()


def run_aio_on_thread(coro):
    if not isawaitable(coro):
        raise TypeError('An awaitable should be provided')

    if not slave_thread:
        _start_thread()
    return asyncio.run_coroutine_threadsafe(coro, slave_loop)


def _get_lock(lock_url, lock_name) -> '(Lock, str)':
    """
    Build or fetch a lock from `lock_url`. Can be a python module having
    a Lock inside. E.g: 'multiprocessing://'

    If the lock needs it, use `lock_name` for the lock name.
    """
    if lock_url.startswith(('redis://', 'rediss://')):
        redis = StrictRedis.from_url(lock_url)
        lock = redis.lock(lock_name)
    elif lock_url:
        lock_module_name = lock_url.partition('://')[0]
        try:
            lock_module = importlib.import_module(lock_module_name)
        except ImportError as e:
            raise RuntimeError("This URL is not supported. "
                               "Only '{redis, multiprocessing, threading, "
                               "dummy_threading}[s]://...' is supported for now") from e
        lock = lock_module.Lock()
    else:
        raise RuntimeError("This URL is not supported. "
                           "Only '{redis, multiprocessing, threading, "
                           "dummy_threading}[s]://...' is supported for now")
    return lock, lock_name


def get_watchdog_lock() -> '(Lock, str)':
    lock_name = os.environ.get('CELERY_SERVERLESS_LOCK_NAME', 'celery_serverless:watchdog')
    lock_url = os.environ.get('CELERY_SERVERLESS_LOCK_URL', None)

    if not lock_url:
        raise ValueError('Environment variable CELERY_SERVERLESS_LOCK_URL must be set. For a dummy lock, use "dummy_threading://"')

    return _get_lock(lock_url, lock_name)


_CLIENT_LOCK = {}

def get_client_lock() -> '(Lock, str)':
    lock_name = os.environ.get('CELERY_SERVERLESS_CLIENT_LOCK_NAME', 'celery_serverless:watchdog-client')
    lock_url = os.environ.get('CELERY_SERVERLESS_CLIENT_LOCK_URL', 'multiprocessing://')

    if _CLIENT_LOCK:
        return _CLIENT_LOCK['lock'], _CLIENT_LOCK['lock_name']

    lock, lock_name = _get_lock(lock_url, lock_name)

    _CLIENT_LOCK.update({'lock': lock, 'lock_name': lock_name})
    return _CLIENT_LOCK['lock'], _CLIENT_LOCK['lock_name']
