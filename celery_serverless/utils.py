# coding: utf-8
import os
import asyncio
from threading import Thread
from inspect import isawaitable
import dummy_threading
import multiprocessing

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


def _get_watchdog_lock(lock_url='', lock_name='', default=dummy_threading.Lock, enforce=True) -> '(Lock, str)':
    lock_name = lock_name or os.environ.get('CELERY_SERVERLESS_LOCK_NAME', 'celery_serverless:watchdog')
    lock_url = lock_url or os.environ.get('CELERY_SERVERLESS_LOCK_URL', '')
    if enforce:
        if lock_url == 'disabled':
            lock_url = ''
        else:
            assert lock_url, 'The CELERY_SERVERLESS_LOCK_URL envvar should be set. Even to "disabled" to disable it.'

    if lock_url.startswith(('redis://', 'rediss://')):
        redis = StrictRedis.from_url(lock_url)
        lock = redis.lock(lock_name)
    elif default and not lock_url:
        lock = default()
    else:
        raise RuntimeError("This URL is not supported. Only 'redis[s]://...' is supported for now")
    return lock, lock_name


_CLIENT_LOCK = {}

def _get_client_lock(lock_url='', lock_name='', default=multiprocessing.Lock, enforce=False):
    if _CLIENT_LOCK and lock_name == _CLIENT_LOCK['lock_name']:
        return _CLIENT_LOCK['lock'], _CLIENT_LOCK['lock_name']

    lock_name = lock_name or os.environ.get('CELERY_SERVERLESS_CLIENT_LOCK_NAME', 'celery_serverless:watchdog-client')
    lock_url = lock_url or os.environ.get('CELERY_SERVERLESS_LOCK_URL', '(unavailable)')
    if enforce:
        if lock_url == 'disabled':
            lock_url = ''
        else:
            assert lock_url, 'The CELERY_SERVERLESS_LOCK_URL envvar should be set. Even to "disabled" to disable it.'

    if default and not lock_url:
        lock = default()
    elif lock_url.startswith(('redis://', 'rediss://')):
        redis = StrictRedis.from_url(lock_url)
        lock = redis.lock(lock_name + '-client')
    else:
        raise RuntimeError("This URL is not supported. Only 'redis[s]://...' is supported for now")

    _CLIENT_LOCK.update({'lock': lock, 'lock_name': lock_name})
    return _CLIENT_LOCK['lock'], _CLIENT_LOCK['lock_name']
